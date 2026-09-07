from __future__ import annotations



from web.dingtalk_pi import DingTalkPiAdapter, presentation_to_dingtalk_card
from web.pi_channel import PiChannelClient


def test_dingtalk_submits_to_authenticated_channel_ingress():
    import json
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Ingress(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def do_POST(self):
            event = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            authorized = self.headers.get("X-Channel-Service-Key") == "synthetic-key"
            valid = event.get("channel") == "dingtalk" and event.get("event_type") == "message"
            status = 202 if authorized and valid else 403
            body = json.dumps({"task": {"task_run_id": "tr_ding"}} if status == 202 else {"code": "forbidden"}).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    with ThreadingHTTPServer(("127.0.0.1", 0), Ingress) as server:
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            adapter = DingTalkPiAdapter(PiChannelClient(base_url="http://127.0.0.1:" + str(server.server_port), service_key="synthetic-key", channel="dingtalk"))
            accepted = adapter.submit_message(event_id="evt_ding", user_id="ding_user", conversation_id="cid_ding", message_id="msg_ding", text="Synthetic")
            assert accepted["task"]["task_run_id"] == "tr_ding"
        finally:
            server.shutdown()
            worker.join()



def test_dingtalk_card_actions_only_return_pi_callback_contract():
    card = presentation_to_dingtalk_card({
        "title": "Forge SQL 审核",
        "markdown": "SELECT 1",
        "actions": [{
            "type": "cancel_task",
            "label": "取消任务",
            "task_run_id": "tr_ding",
            "payload": {},
        }],
    })

    button = card["actionCard"]["btns"][0]
    assert "actionURL" not in button
    assert button["callback"] == {
        "pi_action": True,
        "action_type": "cancel_task",
        "task_run_id": "tr_ding",
        "payload": {},
    }
