# Requirements

## REQ-trusted-business-question-answering-assistant
- source: docs/current-project-state.md
- description: DATA_A7m2Qx9L_START面向已有数据库或数仓的小型数据团队的可信业务问数助手；不要求先完成完整数据治理，而是在真实提问中逐步沉淀和安全复用业务语义。DATA_A7m2Qx9L_END
- acceptance: absent
- scope: DATA_B4v8Nk2P_STARTForge 当前产品定义DATA_B4v8Nk2P_END

## REQ-supported-boundary-correctness
- source: docs/current-project-state.md
- description: DATA_C9r1Tm6W_START正确性承诺限定在受支持边界内：Forge 通过语义、来源、权限、Evidence、确定性编译与审批减少静默错误；不承诺开放世界 100% 正确。DATA_C9r1Tm6W_END
- acceptance: absent
- scope: DATA_D2k7Hs4V_STARTForge 正确性承诺DATA_D2k7Hs4V_END

## REQ-stable-responsibility-boundaries
- source: docs/current-project-state.md
- description: DATA_E6p3Jw8R_START稳定职责边界DATA_E6p3Jw8R_END
- acceptance: DATA_F1x9Lc5N_STARTPi：唯一主 Orchestrator 和 Task 真相源；Forge：可信数据执行层，保留校验、拒绝和失败关闭能力；DATA Skills：专业方法层，不持有任务主状态，不直接获得数据库执行权；Web / 飞书 / 钉钉：渠道与投影层，不创建第二套业务真相源；人工责任：高风险动作、语义确认和生产权限变更不能被 UI 或 Agent 隐式越权。DATA_F1x9Lc5N_END
- scope: DATA_G8d4Vy2K_STARTPi、Forge、DATA Skills、Web、飞书、钉钉、人工责任DATA_G8d4Vy2K_END

## REQ-2026-08-25-023
- source: docs/current-project-state.md
- description: DATA_H5q2Mz7T_START当前有效需求：REQ-2026-08-25-023。DATA_H5q2Mz7T_END
- acceptance: absent
- scope: DATA_J3n8Bp1S_START当前需求与计划DATA_J3n8Bp1S_END

## REQ-s0-design-partner-problem-baseline
- source: docs/current-project-state.md
- description: DATA_K7t4Rf9C_START当前阶段：S0 Design Partner / Problem Baseline。DATA_K7t4Rf9C_END
- acceptance: DATA_L2w6Xh8M_START选择一个有现成数据库或数仓的小型数据团队，固定一个业务域、数据源、语义负责人、真实问题集、隐私/授权边界和现有人工流程基线。DATA_L2w6Xh8M_END
- scope: DATA_M9c1Qv5D_STARTS0 Design Partner / Problem BaselineDATA_M9c1Qv5D_END

## REQ-new-runtime-approval-gate
- source: docs/current-project-state.md
- description: DATA_N4y8Jk2F_START当前不实施新 Runtime。DATA_N4y8Jk2F_END
- acceptance: DATA_P6s3Wm7H_STARTS1–S3、M1A、Agent Runtime、更多 Connector 和企业级扩张均未自动获批；必须由新证据与用户确认开启。DATA_P6s3Wm7H_END
- scope: DATA_Q1b9Tz4R_START新 Runtime、S1–S3、M1A、Agent Runtime、更多 Connector、企业级扩张DATA_Q1b9Tz4R_END

## REQ-w2-user-visual-confirmation
- source: docs/current-project-state.md
- description: DATA_R8m2Cv6K_STARTW2 主体内容规则仍待用户视觉确认。DATA_R8m2Cv6K_END
- acceptance: absent
- scope: DATA_S5h7Np1X_STARTW2 主体内容规则DATA_S5h7Np1X_END

## REQ-atlas-candidate-user-revalidation
- source: docs/current-project-state.md
- description: DATA_T3q9Ld4V_STARTProduct Spine 与完整 Product Shell 的 Atlas candidate 仍有用户复验项。DATA_T3q9Ld4V_END
- acceptance: absent
- scope: DATA_U6k1Yw8B_STARTProduct Spine、完整 Product Shell、Atlas candidateDATA_U6k1Yw8B_END

## REQ-runtime-governance-coverage
- source: docs/current-project-state.md
- description: DATA_V2f8Mr5J_STARTRuntime Governance Coverage 仍为 0%；Contract Coverage 不能替代生产执行覆盖。DATA_V2f8Mr5J_END
- acceptance: absent
- scope: DATA_W9p4Hx3N_STARTRuntime Governance Coverage、Contract Coverage、生产执行覆盖DATA_W9p4Hx3N_END
