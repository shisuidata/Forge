"""Team membership and table ACL in the QueryRun governance database.

Unknown and disabled principals fail closed. An empty ACL denies every table.
"""
from __future__ import annotations

from forge.governance import (
    GovernanceError, PrincipalAccessError, append_event,
    connect_governance, ensure_governance_schema, governance_transaction,
)


class TenantStore:
    def create_team(self, team_id: str, display_name: str = "", *, organization_id: str) -> None:
        if not organization_id:
            raise ValueError("organization_id is required")
        with governance_transaction() as conn:
            row = conn.execute("SELECT organization_id FROM tenant_teams WHERE team_id=?", (team_id,)).fetchone()
            if row:
                if row["organization_id"] != organization_id:
                    raise GovernanceError("team_organization_conflict")
                return
            conn.execute("INSERT INTO tenant_teams (team_id,organization_id,display_name) VALUES (?,?,?)",
                         (team_id,organization_id,display_name or team_id))
            append_event(conn,"team.created",team_id,team_id,{"organization_id":organization_id})

    def list_teams(self) -> list[dict]:
        with connect_governance() as conn:
            ensure_governance_schema(conn)
            rows = conn.execute("SELECT t.team_id,t.organization_id,t.display_name,t.acl_revision,COUNT(u.user_id) member_count "
                                "FROM tenant_teams t LEFT JOIN tenant_users u ON t.team_id=u.team_id "
                                "GROUP BY t.team_id ORDER BY t.team_id").fetchall()
            return [dict(row) for row in rows]

    def set_team(self, user_id: str, team_id: str, display_name: str = "", role: str = "member",
                 *, principal_type: str, status: str) -> None:
        if principal_type not in {"human", "agent", "service"} or role not in {"admin", "reviewer", "member"} or status not in {"active", "disabled"}:
            raise ValueError("invalid principal type, role or status")
        if principal_type != "human" and role != "member":
            raise PrincipalAccessError()
        with governance_transaction() as conn:
            if not conn.execute("SELECT 1 FROM tenant_teams WHERE team_id=?", (team_id,)).fetchone():
                raise GovernanceError("unknown_team", 404)
            prior = conn.execute("SELECT * FROM tenant_users WHERE user_id=?", (user_id,)).fetchone()
            if prior and prior["principal_type"] != principal_type:
                raise GovernanceError("principal_type_immutable")
            name = display_name or user_id
            if prior:
                if all(prior[field] == value for field,value in (("team_id",team_id),("display_name",name),("role",role),("status",status))):
                    return
                conn.execute("UPDATE tenant_users SET team_id=?,display_name=?,role=?,status=?,identity_revision=identity_revision+1,updated_at=datetime('now') WHERE user_id=?",
                             (team_id,name,role,status,user_id))
            else:
                conn.execute("INSERT INTO tenant_users (user_id,team_id,display_name,role,principal_type,status) VALUES (?,?,?,?,?,?)",
                             (user_id,team_id,name,role,principal_type,status))
            append_event(conn,"principal.changed",user_id,user_id,{"team_id":team_id,"role":role,"status":status,"principal_type":principal_type})

    def get_principal(self, principal_id: str) -> dict:
        with connect_governance() as conn:
            ensure_governance_schema(conn)
            row = conn.execute("SELECT u.user_id principal_id,u.team_id workspace_id,t.organization_id,u.role,u.principal_type,u.status,u.identity_revision,t.acl_revision,u.display_name "
                               "FROM tenant_users u JOIN tenant_teams t ON u.team_id=t.team_id WHERE u.user_id=?",(principal_id,)).fetchone()
            if not row or row["status"] != "active":
                raise PrincipalAccessError()
            return dict(row)

    def get_team(self, user_id: str) -> str:
        return self.get_principal(user_id)["workspace_id"]

    def get_team_members(self, team_id: str) -> list[dict]:
        with connect_governance() as conn:
            ensure_governance_schema(conn)
            return [dict(row) for row in conn.execute("SELECT user_id,display_name,role,principal_type,status,identity_revision FROM tenant_users WHERE team_id=?",(team_id,))]

    def get_user_info(self, user_id: str) -> dict | None:
        try:
            principal = self.get_principal(user_id)
        except PrincipalAccessError:
            return None
        return {"user_id":user_id,"team_id":principal["workspace_id"],"display_name":principal["display_name"],
                "role":principal["role"],"principal_type":principal["principal_type"],"status":principal["status"]}

    def set_allowed_tables(self, team_id: str, tables: list[str]) -> None:
        if len(tables) != len(set(tables)):
            raise ValueError("duplicate table names")
        with governance_transaction() as conn:
            if not conn.execute("SELECT 1 FROM tenant_teams WHERE team_id=?",(team_id,)).fetchone():
                raise GovernanceError("unknown_team",404)
            current = {row[0] for row in conn.execute("SELECT table_name FROM team_table_acl WHERE team_id=?",(team_id,))}
            if current == set(tables):
                return
            conn.execute("DELETE FROM team_table_acl WHERE team_id=?",(team_id,))
            conn.executemany("INSERT INTO team_table_acl VALUES (?,?)",((team_id,table) for table in tables))
            conn.execute("UPDATE tenant_teams SET acl_revision=acl_revision+1 WHERE team_id=?",(team_id,))
            append_event(conn,"acl.changed",team_id,team_id,{"tables":sorted(tables)})

    def get_allowed_tables(self, team_id: str) -> list[str]:
        with connect_governance() as conn:
            ensure_governance_schema(conn)
            if not conn.execute("SELECT 1 FROM tenant_teams WHERE team_id=?",(team_id,)).fetchone():
                raise GovernanceError("unknown_team",404)
            return [row[0] for row in conn.execute("SELECT table_name FROM team_table_acl WHERE team_id=? ORDER BY table_name",(team_id,))]

    def get_allowed_tables_for_user(self, user_id: str) -> list[str]:
        return self.get_allowed_tables(self.get_team(user_id))


tenants = TenantStore()
