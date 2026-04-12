# Fluss初步结论

> 文档定位：`Fluss` 第二阶段最小链路 PoC 的初步结论模板  
> 使用方式：在场景 A、场景 B 执行完成后，回填验证结果并形成阶段判断  
> 当前状态：预填版，待 PoC 执行后完善

---

## 一、文档目的

本文件用于在第二阶段最小链路 PoC 执行完成后，快速沉淀以下结论：

- `Fluss` 是否已完成最小闭环验证
- `Log Table`、`PrimaryKey Table`、`Lookup Join` 哪些已经跑通
- 当前是否建议继续进入下一阶段场景化 PoC
- 当前最核心的问题点与风险点是什么

---

## 二、本轮 PoC 范围

本轮 PoC 范围限定在以下两个场景：

### 2.1 场景 A：行为流 + 商品状态补全

验证对象：

- `Log Table`
- `PrimaryKey Table`
- `Lookup Join`

### 2.2 场景 B：库存状态实时表

验证对象：

- `PrimaryKey Table`
- `Streaming Read`
- 最新态读取语义

---

## 三、当前预设结论框架

待 PoC 完成后，建议按以下结构回填。

### 3.1 环境与接入情况

| 项目 | 结果 | 说明 |
|---|---|---|
| `Fluss 0.8.0` 集群部署 | 已完成 | 本地集群版已部署 |
| `Flink 1.20.x` 集成 | 已完成 | 已与 `Fluss Catalog` 打通 |
| `Fluss Catalog` 创建 | 已完成 | 用户已提前验证通过 |
| 独立 PoC database 创建 | 已完成 | 第二阶段 PoC 已创建独立实验库并完成验证 |
| HDFS 文件系统支持 | 已确认 | 需将 `fluss-fs-hdfs-0.8.0-incubating.jar` 放入 `Flink lib` 后重启 `Fluss` 与 `Flink` |

### 3.2 场景 A 结果

| 验证项 | 结果 | 说明 |
|---|---|---|
| 行为流写入 `Log Table` | 已完成 | 写入与读取均正常 |
| 商品状态写入 `PrimaryKey Table` | 已完成 | 主键表创建、写入与读取均正常 |
| `Lookup Join` 跑通 | 已完成 | 结合 `PrimaryKey Table` 的 lookup join 成功实现 |
| 富化结果正确性 | 符合预期 | 富化结果可查询，观察点整体符合预期 |

### 3.3 场景 B 结果

| 验证项 | 结果 | 说明 |
|---|---|---|
| 库存状态写入 `PrimaryKey Table` | 已完成 | 主键状态表写入与抽样读取正常 |
| streaming read 跑通 | 已完成 | 可正常查询到 streaming 结果 |
| 最新态读取语义符合预期 | 符合预期 | 状态覆盖与最新态语义符合 PoC 预期 |

---

## 四、建议的判断维度

PoC 完成后，建议从以下 5 个维度做判断。

### 4.1 集成友好度

关注点：

- 与 `Flink SQL Client` 集成是否顺畅
- 建表、写入、读取和 join 的 SQL 心智是否清晰
- 对现有团队学习成本是否可接受

### 4.2 表模型适配度

关注点：

- `Log Table` 是否适合事件流
- `PrimaryKey Table` 是否适合实时状态流
- 是否明显比当前链路在某个局部场景更自然

### 4.3 Lookup 场景价值

关注点：

- lookup join 是否稳定
- 维表主键要求是否可接受
- 对我们后续行为流、库存状态 enrichment 是否有现实价值

### 4.4 运维与治理复杂度

关注点：

- 新组件是否带来额外复杂度
- 是否容易接入现有监控、治理、巡检体系
- 是否需要单独补较多运行保障工作

### 4.5 是否建议继续推进

关注点：

- 只是“能跑”，还是“值得继续”
- 是否适合进入更贴业务的下一阶段 PoC

---

## 五、预设结论口径

### 5.1 若 PoC 跑通良好

可参考以下结论口径：

`Fluss 0.8.0` 已在当前 `Flink 1.20.x` 测试环境中完成最小链路 PoC 验证。  
`Log Table`、`PrimaryKey Table`、`Lookup Join` 均具备继续深入验证的价值。  
其中，`PrimaryKey Table` 对实时状态层与实时 enrichment 场景的适配度较高，建议进入下一阶段的轻量场景化 PoC。

### 5.2 若 PoC 部分通过

可参考以下结论口径：

`Fluss` 已完成部分最小能力验证，但在集成细节、读写行为或 lookup 场景上仍有待进一步确认。  
当前建议继续维持旁路预研，不进入更复杂业务试点，优先先补齐问题定位与版本适配验证。

### 5.3 若 PoC 不理想

可参考以下结论口径：

`Fluss` 当前虽具备一定技术潜力，但在现阶段测试环境下尚未证明其对本项目形成明确增益。  
建议继续观察社区与版本演进，短期内不作为二期优先引入方向。

---

## 六、当前初步判断

基于本轮第二阶段最小链路 PoC 的实际执行结果，当前可以给出如下判断：

- `Fluss 0.8.0` 已在本地测试环境中完成最小链路 PoC
- `Flink 1.20.x` 与 `Fluss 0.8.0` 的集成兼容性整体良好
- `Fluss Catalog`、database、`Log Table`、`PrimaryKey Table` 均已验证通过
- 场景 A、场景 B 均已跑通
- `Lookup Join` 已成功实现
- `Fluss` 对事件流承载、主键状态表承载、实时 enrichment 的能力已初步得到验证
- 当前已识别并解决一项关键环境问题：
  - 当底层使用 `hdfs://` 时，`Flink` 侧同样需要具备对应文件系统支持
  - 本次已通过将 `fluss-fs-hdfs-0.8.0-incubating.jar` 放入 `Flink lib` 目录并重启完成修复
- 当前已识别并解决一项 PoC 验证方式问题：
  - `print` connector 只能作为 sink，不能直接 `SELECT`
  - 本次已调整为可查询的 `Fluss` 结果表进行验证
- 最值得继续验证的是：
  - `Log Table`
  - `PrimaryKey Table`
  - `Lookup Join`
  - 更贴业务语义的增强数据方案与场景化验证
- 若这三块全部跑通，`Fluss` 就具备进入下一阶段轻量业务场景验证的资格

---

## 七、下一阶段建议判定标准

满足以下条件，建议继续推进：

1. 场景 A 跑通
2. 场景 B 跑通
3. 无重大版本兼容问题
4. `Lookup Join` 具备现实价值
5. 团队认为引入成本与潜在收益基本匹配

本轮判断：

- 上述条件已全部满足
- 因此建议进入下一阶段“与现有场景结合验证”
- 下一阶段建议引入更业务化、更可控的增强测试数据方案

若后续场景化验证只满足部分，则建议：

- 继续保留旁路预研定位
- 不急于将其纳入二期核心交付范围

---

## 八、配套文档

- [Fluss预研规划.md](/d:/workspace/realtime-lakehouse/Fluss预研规划.md)
- [Fluss预研笔记.md](/d:/workspace/realtime-lakehouse/Fluss预研笔记.md)
- [Fluss技术概念映射表.md](/d:/workspace/realtime-lakehouse/Fluss技术概念映射表.md)
- [Fluss最小验证路线图.md](/d:/workspace/realtime-lakehouse/Fluss最小验证路线图.md)
- [Fluss PoC 操作验证文档.md](/d:/workspace/realtime-lakehouse/Fluss%20PoC%20操作验证文档.md)
- [Fluss 测试数据方案.md](/d:/workspace/realtime-lakehouse/Fluss%20测试数据方案.md)
