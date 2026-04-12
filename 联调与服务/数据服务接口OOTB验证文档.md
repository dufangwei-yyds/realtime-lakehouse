# 数据服务接口 完整Java代码OOTB验证文档

> 适用范围：基于 [实时湖仓ADS建模实现方案.md](/d:/workspace/realtime-lakehouse/实时湖仓ADS建模实现方案.md) 中的接口设计与当前工作区提供的 `ads-api-demo` 示例工程，完成接口模型的开箱即用验证。  
> 当前目标：你可以不自己搭代码骨架，直接参考本地现成 Java 代码与本文档，完成从配置 Doris 到启动接口、调用接口、核对结果的完整验证。  
> 工程路径：`/d:/workspace/realtime-lakehouse/ads-api-demo`

---

## 一、你将验证什么

本次验证的核心不是“接口框架能不能跑”，而是以下 4 件事：

1. Java 接口代码是否能正确连接 Doris `ads` 库
2. 两个接口是否能正确返回 ADS 结果表中的数据
3. 返回结构、分页、过滤、排序是否符合 ADS 文档中的设计
4. Java DTO 字段是否和 ADS 表字段一一对应

本次验证对象：

- 调拨建议接口：`GET /api/ads/inventory/dispatch-suggestions`
- 追单建议接口：`GET /api/ads/hot-sku/reorder-suggestions`

---

## 二、已提供的完整 Java 代码清单

当前工作区已提供一个完整可参考的 Spring Boot 示例工程：

`/d:/workspace/realtime-lakehouse/ads-api-demo`

### 2.1 主要文件

| 类型 | 文件 |
|---|---|
| Maven 工程 | [pom.xml](/d:/workspace/realtime-lakehouse/ads-api-demo/pom.xml) |
| 应用入口 | [AdsApiApplication.java](/d:/workspace/realtime-lakehouse/ads-api-demo/src/main/java/com/fashion/adsapi/AdsApiApplication.java) |
| 配置文件 | [application.yml](/d:/workspace/realtime-lakehouse/ads-api-demo/src/main/resources/application.yml) |
| 公共返回 | [ApiResponse.java](/d:/workspace/realtime-lakehouse/ads-api-demo/src/main/java/com/fashion/adsapi/common/ApiResponse.java) |
| 分页模型 | [PageRequest.java](/d:/workspace/realtime-lakehouse/ads-api-demo/src/main/java/com/fashion/adsapi/common/PageRequest.java) |
| 分页模型 | [PageResult.java](/d:/workspace/realtime-lakehouse/ads-api-demo/src/main/java/com/fashion/adsapi/common/PageResult.java) |
| 全局异常 | [GlobalExceptionHandler.java](/d:/workspace/realtime-lakehouse/ads-api-demo/src/main/java/com/fashion/adsapi/common/GlobalExceptionHandler.java) |
| 调拨建议接口 | [InventoryDispatchController.java](/d:/workspace/realtime-lakehouse/ads-api-demo/src/main/java/com/fashion/adsapi/inventory/InventoryDispatchController.java) |
| 调拨建议服务 | [InventoryDispatchService.java](/d:/workspace/realtime-lakehouse/ads-api-demo/src/main/java/com/fashion/adsapi/inventory/InventoryDispatchService.java) |
| 调拨建议仓储 | [InventoryDispatchRepository.java](/d:/workspace/realtime-lakehouse/ads-api-demo/src/main/java/com/fashion/adsapi/inventory/InventoryDispatchRepository.java) |
| 追单建议接口 | [HotSkuReorderController.java](/d:/workspace/realtime-lakehouse/ads-api-demo/src/main/java/com/fashion/adsapi/hotsku/HotSkuReorderController.java) |
| 追单建议服务 | [HotSkuReorderService.java](/d:/workspace/realtime-lakehouse/ads-api-demo/src/main/java/com/fashion/adsapi/hotsku/HotSkuReorderService.java) |
| 追单建议仓储 | [HotSkuReorderRepository.java](/d:/workspace/realtime-lakehouse/ads-api-demo/src/main/java/com/fashion/adsapi/hotsku/HotSkuReorderRepository.java) |

---

## 三、前置条件

### 3.1 环境要求

- JDK `17`
- Maven `3.9+`
- Doris 可访问
- `ads` 库中以下表已完成建表并有数据：
  - `ads.ads_inventory_dispatch_api`
  - `ads.ads_hot_sku_reorder_api`

### 3.2 Doris 连通性先做 SQL 验证

你说 SQL 结果验证已经完成，这里仍建议保留这一步作为启动前最后确认：

```sql
SELECT COUNT(*) FROM ads.ads_inventory_dispatch_api;
SELECT COUNT(*) FROM ads.ads_hot_sku_reorder_api;
```

若都大于 `0`，再进入接口启动环节。

---

## 四、Step 1：配置 Java 工程

### 4.1 打开配置文件

打开：

[application.yml](/d:/workspace/realtime-lakehouse/ads-api-demo/src/main/resources/application.yml)

### 4.2 核对以下配置

```yaml
spring:
  datasource:
    driver-class-name: com.mysql.cj.jdbc.Driver
    url: jdbc:mysql://192.168.63.128:9030/ads?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai
    username: root
    password: ""
```

### 4.3 你需要确认的点

- `192.168.63.128` 是否是当前 Doris FE 地址
- `9030` 是否是当前 Doris MySQL 查询端口
- `username/password` 是否正确

如果密码不是空串，请改成你当前环境实际值。

---

## 五、Step 2：启动示例工程

### 5.1 命令行启动方式

在目录：

`d:\workspace\realtime-lakehouse\ads-api-demo`

执行：

```powershell
mvn spring-boot:run
```

### 5.2 预期结果

启动成功后，控制台应看到类似信息：

```text
Started AdsApiApplication in ...
Tomcat started on port 8080
```

### 5.3 健康检查

浏览器或 Postman 访问：

```text
http://localhost:8080/actuator/health
```

预期返回：

```json
{"status":"UP"}
```

如果访问 `/actuator/health` 报类似错误：

`No static resource actuator/health`

说明工程中缺少 `spring-boot-starter-actuator` 依赖。当前示例工程已补齐该依赖；如果你在本地启动前已经执行过一次依赖解析，请重新执行一次启动命令，让 Maven 拉取新依赖。

---

## 六、Step 3：验证调拨建议接口

### 6.1 基础请求

在浏览器、Postman 或 curl 中访问：

```text
http://localhost:8080/api/ads/inventory/dispatch-suggestions?pageNo=1&pageSize=20
```

### 6.2 预期返回结构

```json
{
  "code": "0",
  "message": "success",
  "data": {
    "total": 3,
    "pageNo": 1,
    "pageSize": 20,
    "records": [
      {
        "statMinute": "2026-04-02T10:00:00",
        "storeId": "S001",
        "warehouseId": "W001",
        "skuId": "P2026S001-BLK-M",
        "skuName": "春日碎花连衣裙",
        "atpQty": -2,
        "demandQty": 0,
        "dispatchPriorityScore": 100.0000,
        "suggestAction": "URGENT_DISPATCH",
        "triggerReason": "当前可承诺库存不足",
        "updateTime": "2026-04-02T10:05:00"
      }
    ]
  }
}
```

### 6.3 带过滤参数验证

```text
http://localhost:8080/api/ads/inventory/dispatch-suggestions?storeId=S001&pageNo=1&pageSize=20
```

### 6.4 验证点

- 是否返回 `ApiResponse<PageResult<InventoryDispatchSuggestionDTO>>`
- `records` 内字段是否和 [InventoryDispatchSuggestionDTO.java](/d:/workspace/realtime-lakehouse/ads-api-demo/src/main/java/com/fashion/adsapi/inventory/InventoryDispatchSuggestionDTO.java) 一致
- `dispatchPriorityScore` 是否按降序返回
- 过滤 `storeId` 后结果是否收敛

---

## 七、Step 4：验证追单建议接口

### 7.1 基础请求

```text
http://localhost:8080/api/ads/hot-sku/reorder-suggestions?pageNo=1&pageSize=20
```

### 7.2 预期返回结构

```json
{
  "code": "0",
  "message": "success",
  "data": {
    "total": 10,
    "pageNo": 1,
    "pageSize": 20,
    "records": [
      {
        "statHour": "2026-04-02T10:00:00",
        "skuId": "P2026S001-BLK-M",
        "skuName": "春日碎花连衣裙",
        "brandName": "TBE",
        "hotScore": 531.9000,
        "rankNo": 1,
        "reorderSuggestFlag": 1,
        "reorderLevel": "HIGH",
        "triggerReason": "热度等级=A; 趋势标签=销量快速增长; 排名=1",
        "stockSupportFlag": 1,
        "updateTime": "2026-04-02T10:05:00"
      }
    ]
  }
}
```

### 7.3 带过滤参数验证

```text
http://localhost:8080/api/ads/hot-sku/reorder-suggestions?brandName=TBE&pageNo=1&pageSize=20
```

### 7.4 验证点

- 是否返回 `ApiResponse<PageResult<HotSkuReorderDTO>>`
- `records` 字段是否与 [HotSkuReorderDTO.java](/d:/workspace/realtime-lakehouse/ads-api-demo/src/main/java/com/fashion/adsapi/hotsku/HotSkuReorderDTO.java) 一致
- `hotScore` 是否按降序返回
- `rankNo` 是否稳定
- 过滤 `brandName` 后结果是否收敛

---

## 八、Step 5：分页验证

### 8.1 调拨建议分页验证

先访问第一页：

```text
http://localhost:8080/api/ads/inventory/dispatch-suggestions?pageNo=1&pageSize=2
```

再访问第二页：

```text
http://localhost:8080/api/ads/inventory/dispatch-suggestions?pageNo=2&pageSize=2
```

### 8.2 预期结果

- 第一页和第二页 `records` 不应完全重复
- `total` 应保持一致
- `pageNo/pageSize` 应正确反映当前页

### 8.3 追单建议分页验证

```text
http://localhost:8080/api/ads/hot-sku/reorder-suggestions?pageNo=1&pageSize=2
```

```text
http://localhost:8080/api/ads/hot-sku/reorder-suggestions?pageNo=2&pageSize=2
```

同样验证：

- 是否翻页正常
- 是否按默认排序持续稳定

---

## 九、Step 6：SQL 与接口返回核对

### 9.1 调拨建议核对

先执行 SQL：

```sql
SELECT
    stat_minute,
    store_id,
    warehouse_id,
    sku_id,
    sku_name,
    atp_qty,
    demand_qty,
    dispatch_priority_score,
    suggest_action,
    trigger_reason,
    update_time
FROM ads.ads_inventory_dispatch_api
ORDER BY dispatch_priority_score DESC, stat_minute DESC
LIMIT 2;
```

再访问接口：

```text
http://localhost:8080/api/ads/inventory/dispatch-suggestions?pageNo=1&pageSize=2
```

核对点：

- SQL 第一条是否等于接口 `records[0]`
- SQL 第二条是否等于接口 `records[1]`

### 9.2 追单建议核对

先执行 SQL：

```sql
SELECT
    stat_hour,
    sku_id,
    sku_name,
    brand_name,
    hot_score,
    rank_no,
    reorder_suggest_flag,
    reorder_level,
    trigger_reason,
    stock_support_flag,
    update_time
FROM ads.ads_hot_sku_reorder_api
ORDER BY hot_score DESC, rank_no ASC
LIMIT 2;
```

再访问接口：

```text
http://localhost:8080/api/ads/hot-sku/reorder-suggestions?pageNo=1&pageSize=2
```

核对点：

- SQL 结果与接口返回是否一致
- DTO 字段映射是否正确

---

## 十、Step 7：字段映射核对清单

### 10.1 调拨建议接口字段映射

| ADS 字段 | DTO 字段 |
|---|---|
| `stat_minute` | `statMinute` |
| `store_id` | `storeId` |
| `warehouse_id` | `warehouseId` |
| `sku_id` | `skuId` |
| `sku_name` | `skuName` |
| `atp_qty` | `atpQty` |
| `demand_qty` | `demandQty` |
| `dispatch_priority_score` | `dispatchPriorityScore` |
| `suggest_action` | `suggestAction` |
| `trigger_reason` | `triggerReason` |
| `update_time` | `updateTime` |

### 10.2 追单建议接口字段映射

| ADS 字段 | DTO 字段 |
|---|---|
| `stat_hour` | `statHour` |
| `sku_id` | `skuId` |
| `sku_name` | `skuName` |
| `brand_name` | `brandName` |
| `hot_score` | `hotScore` |
| `rank_no` | `rankNo` |
| `reorder_suggest_flag` | `reorderSuggestFlag` |
| `reorder_level` | `reorderLevel` |
| `trigger_reason` | `triggerReason` |
| `stock_support_flag` | `stockSupportFlag` |
| `update_time` | `updateTime` |

---

## 十一、常见问题排查

### 11.1 应用启动失败

优先检查：

- JDK 是否为 `17`
- Maven 依赖是否下载成功
- `application.yml` 中 Doris 地址、账号、密码是否正确

### 11.2 启动成功但接口返回 `5001`

优先检查：

- Doris `ads` 库是否可访问
- 目标表是否存在
- SQL 在 Doris 中是否能单独执行

### 11.3 接口返回成功但 `records` 为空

优先检查：

- ADS 表是否有数据
- 过滤条件是否过严
- 当前测试数据时间范围是否在预期窗口内

### 11.4 分页结果重复

优先检查：

- 默认排序是否仍是：
  - 调拨建议：`dispatch_priority_score DESC, stat_minute DESC`
  - 追单建议：`hot_score DESC, rank_no ASC`

### 11.5 DTO 字段为空

优先检查：

- SQL 里是否已查询该字段
- `RowMapper` 是否正确映射
- ADS 表字段名是否已变化但代码未同步

---

## 十二、验证通过标准

满足以下条件即可认为本次接口模型 OOTB 验证通过：

- Java 示例工程可成功启动
- 两个接口均可正常返回 `200`
- 返回结构符合 `ApiResponse<PageResult<T>>`
- SQL 结果与接口返回字段一致
- 分页、排序、过滤符合设计预期
- DTO 与 ADS 字段一一对应

---

## 十三、下一步建议

本轮验证通过后，下一步就可以进入 ADS 第一轮“贴真实环境”校对，优先处理：

1. 是否需要按你当前真实字段进一步修正 DTO / Query 参数
2. 是否需要把示例工程改造成更贴近你们实际习惯的 MyBatis 版本
3. 是否需要增加接口级验证记录模板，方便逐项打勾留痕
