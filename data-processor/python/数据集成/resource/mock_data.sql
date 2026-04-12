create database ecommerce_db;
use ecommerce_db;
-- orders 表（订单主表）
CREATE TABLE orders (
    order_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    order_no VARCHAR(50) NOT NULL UNIQUE COMMENT '订单编号',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    sku_id VARCHAR(20) NOT NULL COMMENT 'SKU编码',
    size VARCHAR(10) NOT NULL COMMENT '尺码',
    color VARCHAR(20) NOT NULL COMMENT '颜色',
    quantity INT NOT NULL COMMENT '数量',
    order_amount DECIMAL(10,2) NOT NULL COMMENT '订单金额',
    order_status TINYINT NOT NULL COMMENT '订单状态(1:待支付,2:已支付,3:已发货,4:已完成,5:已取消)',
    create_time DATETIME NOT NULL COMMENT '创建时间',
    update_time DATETIME NOT NULL COMMENT '更新时间',
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (sku_id) REFERENCES goods(sku_id)
);

-- order_status_history 表（订单状态变更历史表）
CREATE TABLE order_status_history (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    order_id BIGINT NOT NULL COMMENT '订单ID',
    old_status TINYINT NOT NULL COMMENT '原状态',
    new_status TINYINT NOT NULL COMMENT '新状态',
    change_time DATETIME NOT NULL COMMENT '变更时间',
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

create database member_db;
use member_db;
-- users 表（用户信息表）
CREATE TABLE users (
    user_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(50) NOT NULL UNIQUE COMMENT '用户名',
    phone VARCHAR(20) NOT NULL UNIQUE COMMENT '手机号',
    gender TINYINT NOT NULL COMMENT '性别(0:未知,1:男,2:女)',
    birth_date DATE NOT NULL COMMENT '出生日期',
    register_time DATETIME NOT NULL COMMENT '注册时间',
    preferred_style VARCHAR(50) NOT NULL COMMENT '偏爱风格',
    preferred_size VARCHAR(10) NOT NULL COMMENT '偏爱尺码',
    vip_level TINYINT NOT NULL COMMENT 'VIP等级(1-5级)',
    is_active TINYINT NOT NULL DEFAULT 1 COMMENT '是否活跃(0:否,1:是)'
);

create database erp_db;
use erp_db;
-- goods 表（商品信息表）
CREATE TABLE goods (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    sku_id VARCHAR(20) NOT NULL UNIQUE COMMENT 'SKU编码',
    spu_id VARCHAR(20) NOT NULL COMMENT 'SPU编码',
    goods_name VARCHAR(255) NOT NULL COMMENT '商品名称',
    category VARCHAR(50) NOT NULL COMMENT '分类',
    brand VARCHAR(50) NOT NULL COMMENT '品牌',
    material VARCHAR(50) NOT NULL COMMENT '材质',
    season VARCHAR(10) NOT NULL COMMENT '季节',
    style VARCHAR(50) NOT NULL COMMENT '风格',
    size_range TEXT NOT NULL COMMENT '尺码范围',
    color_list TEXT NOT NULL COMMENT '颜色列表',
    price DECIMAL(10,2) NOT NULL COMMENT '售价',
    cost_price DECIMAL(10,2) NOT NULL COMMENT '成本价',
    status TINYINT NOT NULL DEFAULT 1 COMMENT '状态(1:正常)',
    create_time DATETIME NOT NULL COMMENT '创建时间',
    update_time DATETIME NOT NULL COMMENT '更新时间'
);

-- warehouses 表（仓库信息表）
CREATE TABLE warehouses (
    warehouse_id INT PRIMARY KEY,
    warehouse_name VARCHAR(50) NOT NULL COMMENT '仓库名称',
    region VARCHAR(10) NOT NULL COMMENT '区域',
    type TINYINT NOT NULL COMMENT '类型(1:总仓,2:分仓)'
);

-- inventory 表（库存表）
CREATE TABLE inventory (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    sku_id VARCHAR(20) NOT NULL COMMENT 'SKU编码',
    size VARCHAR(10) NOT NULL COMMENT '尺码',
    warehouse_id INT NOT NULL COMMENT '仓库ID',
    available_quantity INT NOT NULL DEFAULT 0 COMMENT '可用库存',
    locked_quantity INT NOT NULL DEFAULT 0 COMMENT '锁定库存',
    update_time DATETIME NOT NULL COMMENT '更新时间',
    FOREIGN KEY (sku_id) REFERENCES goods(sku_id),
    FOREIGN KEY (warehouse_id) REFERENCES warehouses(warehouse_id)
);
