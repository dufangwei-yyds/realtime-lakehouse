-- P_L_DWA_I_MFG_ATTEND_HOUR_D

/**
  总体解读：
  1. 基础工时处理： 获取标准化的员工工时数据
  2. IOP/NPI/Rework等专项工时处理： 统计特殊场景下的工时
  3. 开线时长与编制人数： 支撑工时分配比例计算
  4. 工时比例计算： 实现工时的合理分摊
  5. 最终结果表： 输出可用于财务核算的工时报表
 */

-- 1. 获取基础工时
/**
  * 来源表：
     lcfc.dwd_i_hr_detail_man_hour_d： 员工详细工时明细
     lcfc.dw_a_hr_staff_base_infor_d： 员工基本信息
     lcfc.dwd_a_org_to_pu_d： 组织结构信息
     代码逻辑：
      1. 提取特定日期范围内的员工工时数据，并按部门、PU（生产单元）、流程等维度进行分类
      2. 使用 case when 对部门名称进行标准化处理
      3. 过滤掉无效或不相关的组织层级
     输出表：
     lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_01
 */
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_01
select
    t1.day_id as stat_date,
    t1.id_no_sz,
    case when t2.sec_org_desc = 'BOX 生产部' then 'BOX'
         when t2.sec_org_desc = 'SMT 生产部' then 'SMT'
    end as sec_org_desc,
    t2.thr_org_desc,
    t2.four_org_desc,
    coalesce(t6.process,'-999') as group_process,
    t6.pu_name,
    t1.inputhours as work_hour
from lcfc.dwd_i_hr_detail_man_hour_d t1
left join lcfc.dw_a_hr_staff_base_infor_d t2
  on t1.id_no_sz = t2.id_no_sz
 and t1.day_id = t2.day_id
 and t2.data_source = 'CN'
left join (
    select distinct
        t2_org_name,
        t3_org_name,
        t4_org_name,
        org_name,
        pu_name,
        case when process = 'Option' then 'SUB'
             when process = 'OBE' then 'PACK'
             else process
        end as process
    from lcfc.dwd_a_org_to_pu_d
) t6
  on t2.four_org_desc = t6.org_name
where t1.day_id >= '${first_day_of_month}'
  and t1.day_id >= '2024-04-21'
  and t1.day_id <= '${now_1}'
  and t1.inputhours <> 0
  and t2.sec_org_desc in ('BOX 生产部','SMT 生产部')
  and t2.thr_org_desc in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
  and t2.four_org_desc not in ('BOX 组合生产 Ⅰ 组装设备维护组','BOX 组合生产 Ⅱ 设备维护组','BOX 包装生产 Ⅰ 设备维护组','BOX 包装生产 Ⅱ 设备维护组','BOX 物料管理 Ⅱ DT&Lark 组','BOX 部件加工 Ⅱ DT/Lark 生产组','BOX 生产检验 Ⅱ DT/Lark 测试组','BOX 物料管理 Ⅰ Repair 组','BOX 物料管理 Ⅰ CB Repair 组','BOX 物料管理 Ⅱ Repair 组','BOX 物料管理 Ⅱ CB Repair 组','BOX 物料管理 Ⅰ 账务组','BOX 物料管理 Ⅰ CB 账务组','BOX 物料管理 Ⅱ 账务组','BOX 物料管理 Ⅱ CB 账务组','SMT 账务 Ⅲ 组','SMT 账务 Ⅳ 组','BOX 库存管理组','SMT 程式 Ⅲ 组','SMT 程式 Ⅳ 组','SMT 维护 Ⅲ 组','SMT 维护 Ⅳ 组','SMT 保养 Ⅲ 组','SMT 保养 Ⅳ 组','SMT ATE 工程 Ⅲ 组','SMT ATE 工程 Ⅳ 组','SMT ATE 修护 Ⅲ 组','SMT ATE 修护 Ⅳ 组','SMT AOI 目视 Ⅲ 组','SMT AOI 目视 Ⅳ 组','SMT 自动化组','PCBA Ⅲ 维护裁板组','PCBA Ⅳ 维护裁板组','SMT 账务 Ⅲ 组','SMT 账务 Ⅳ 组','SMT 海外业务 Ⅳ 组','SMT 修护物料管理 Ⅲ 组','SMT 修护物料管理 Ⅳ 组')
;

-- 2.处理无制程段的工时
/**
  来源表：
  lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_01
  代码逻辑：
    筛选没有制程段的记录，根据部门和组织名称进行过滤
  输出表：
  lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_011
 */
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_011
select
    t1.stat_date,
    t1.sec_org_desc,
    t1.pu_name,
    t1.group_process,
    t1.work_hour
from lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_01 t1
where sec_org_desc in ('BOX')
  and four_org_desc not like '%MTY%'
  and four_org_desc not like '%Option%'
  and four_org_desc not like '%Service%'
  and four_org_desc not like '%Remote Site%'

union all

select
    t1.stat_date,
    t1.sec_org_desc,
    t1.pu_name,
    t1.group_process,
    t1.work_hour
from lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_01 t1
where sec_org_desc in ('SMT')
  and four_org_desc not like '%MTY%'
  and four_org_desc not like '%FRU%'
  and four_org_desc not like '%L6%'
  ;

-- 3.工时分配计算
/**
  来源表：
  lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_011
  代码逻辑：
   1. 计算不同制程段之间的工时分配比例
   2. 使用子查询计算总工时，并通过左连接进行加权分配
  输出表：
  lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_02
 */
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_02
select
    t1.stat_date,
    t1.sec_org_desc,
    --t1.thr_org_desc,
    --t1.four_org_desc,
    t1.pu_name,
    t3.group_process,
    sum(t1.work_hour * coalesce(t3.work_hour / t2.work_hour,0)) as work_hour
from lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_011 t1
left join (
     select
        t1.stat_date,
        t1.sec_org_desc,
        --t1.thr_org_desc,
        --t1.four_org_desc,
        t1.pu_name,
        sum(t1.work_hour) as work_hour
    from lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_011 t1
    where t1.group_process <> '-999'
    group by t1.stat_date,
        t1.sec_org_desc,
        --t1.thr_org_desc,
        --t1.four_org_desc,
        t1.pu_name
    ) t2
  on t1.stat_date = t2.stat_date
 and t1.sec_org_desc = t2.sec_org_desc
 --and t1.thr_org_desc = t2.thr_org_desc
 and t1.pu_name = t2.pu_name
left join (
    select
        t1.stat_date,
        t1.sec_org_desc,
        --t1.thr_org_desc,
        --t1.four_org_desc,
        t1.group_process,
        t1.pu_name,
        sum(t1.work_hour) as work_hour
    from lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_011 t1
    where t1.group_process <> '-999'
    group by t1.stat_date,
        t1.sec_org_desc,
        --t1.thr_org_desc,
        --t1.four_org_desc,
        t1.group_process,
        t1.pu_name
    ) t3
  on t1.stat_date = t3.stat_date
 and t1.sec_org_desc = t3.sec_org_desc
 --and t1.thr_org_desc = t3.thr_org_desc
 and t1.pu_name = t3.pu_name
where t1.group_process = '-999'
group by t1.stat_date,
    t1.sec_org_desc,
    --t1.thr_org_desc,
    --t1.four_org_desc,
    t1.pu_name,
    t3.group_process
;

-- 4.工时合计
/**
  来源表：
  lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_011 和 lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_02
  代码逻辑：
    合并两个临时表中的数据，按部门、PU、制程段等维度汇总工时
  输出表：
  lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_03
 */
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_03
select
    t1.stat_date,
    t1.sec_org_desc,
    --t1.thr_org_desc,
    --t1.four_org_desc,
    t1.group_process,
    t1.pu_name,
    sum(t1.work_hour) as work_hour
from (
    select
        t1.stat_date,
        t1.sec_org_desc,
        --t1.thr_org_desc,
        --t1.four_org_desc,
        t1.group_process,
        t1.pu_name,
        t1.work_hour
    from lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_011 t1
    where t1.group_process <> '-999'
    union all
    select
        t1.stat_date,
        t1.sec_org_desc,
        --t1.thr_org_desc,
        --t1.four_org_desc,
        t1.group_process,
        t1.pu_name,
        t1.work_hour
    from lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_02 t1
    ) t1
group by t1.stat_date,
    t1.sec_org_desc,
    --t1.thr_org_desc,
    --t1.four_org_desc,
    t1.group_process,
    t1.pu_name

-- 5.IOP（内部借调）工时
/**
  来源表：
  lcfc.dwd_i_hr_iop_mh_d：IOP工时记录
  代码逻辑：
   1. 分别处理借出和借入的工时数据
   2. 根据 iop_type 判断是否为生效记录
   3. 按照部门、PU、model、阶段等字段进行分组统计
  输出表：
  lcfc.tmp_dwa_i_mfg_attend_iop_hour_d_01
 */
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_iop_hour_d_01
select
    from_unixtime(unix_timestamp(work_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd')  as stat_date,
    case when loan_t2 in ('PU1','PU2') then 'BOX'
         when loan_t2 in ('PU3','PU4') then 'SMT'
    end as sec_org_desc,
    loan_t2 as pu_name,
    loan_t3 as t3_org_name,
    loan_t4 as t4_org_desc,
    case when loan_region = 'PCB' then 'PCB' when loan_region = 'Option' then 'SUB' else loan_region end as group_process,
    coalesce(model_name,'') as model_name,
    totals as loan_hour,
    0 as borrow_hour,
    phase1 as phase
from lcfc.dwd_i_hr_iop_mh_d t1
where t1.work_date >= '${first_day_of_month}'
  and t1.work_date <= '${now_1}'
  and t1.check_flag = '生效'
  and t1.iop_type in ('IOP','REWORK')
  and t1.loan_t2 in ('PU1','PU2','PU3','PU4')
  and t1.loan_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
union all
select
    from_unixtime(unix_timestamp(work_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd')  as stat_date,
    case when borrow_t2 in ('PU1','PU2') then 'BOX'
         when borrow_t2 in ('PU3','PU4') then 'SMT'
    end as sec_org_desc,
    borrow_t2 as pu_name,
    borrow_t3 as t3_org_name,
    borrow_t4 as t4_org_desc,
    case when borrow_region = 'PCB' then 'PCBA' when borrow_region = 'Option' then 'SUB' else borrow_region end as group_process,
    coalesce(model_name,'') as model_name,
    0 as loan_hour,
    totals as borrow_hour,
    phase1 as phase
from lcfc.dwd_i_hr_iop_mh_d t1
where t1.work_date >= '${first_day_of_month}'
  and t1.work_date <= '${now_1}'
  and t1.check_flag = '生效'
  and t1.iop_type in ('IOP','REWORK')
  and t1.borrow_t2 in ('PU1','PU2','PU3','PU4')
  and t1.borrow_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
;

-- 6.MTY_RS_SRV工时
/**
  来源表：
  lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_01 和 lcfc.dwd_i_hr_iop_mh_d
  代码逻辑：
   1. 分别处理 SMT 和 BOX 部门下的 MTY（模块测试）、FRU（故障更换）、R/S（远程/服务）等特殊工时
   2. 结合借调数据，调整最终工时
  输出表：
  lcfc.tmp_dwa_i_mfg_attend_smt_mty_hour_d_01
  lcfc.tmp_dwa_i_mfg_attend_smt_fru_hour_d_01
  lcfc.tmp_dwa_i_mfg_attend_smt_svc_iop_hour_d_01
  lcfc.tmp_dwa_i_mfg_attend_smt_rs_hour_d_01
  lcfc.tmp_dwa_i_mfg_attend_box_mty_hour_d_01
  lcfc.tmp_dwa_i_mfg_attend_box_svc_iop_hour_d_01
 */
    --SMT MTY
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_smt_mty_hour_d_01
select
    '${first_day_of_month}' as stat_date,
    'MTY' as model_name,
    t1.sec_org_desc,
    t1.pu_name,
    t1.work_hour + coalesce(t2.work_hour,0) - coalesce(t3.work_hour,0) as work_hour
from (
    select
        t1.sec_org_desc,
        --t1.thr_org_desc,
        --t1.four_org_desc,
        t1.pu_name,
        sum(t1.work_hour) as work_hour
    from lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_01 t1
    where four_org_desc  like '%MTY%'
      and pu_name in ('PU4','PU3')
    group by t1.sec_org_desc,t1.pu_name
    ) t1
left join (
    select
        'SMT' as sec_org_desc,
        t1.borrow_t2 as pu_name,
        sum(totals) as work_hour
    from lcfc.dwd_i_hr_iop_mh_d t1
    where t1.work_date >= '${first_day_of_month}'
      and t1.work_date <= '${now_1}'
      and t1.check_flag = '生效'
      and t1.iop_type in ('IOP')
      and t1.borrow_t2 in ('PU3','PU4')
      and t1.borrow_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
      and t1.borrow_t4 in ('MTY')
    group by t1.borrow_t2
    ) t2
  on t1.pu_name = t2.pu_name
left join (
    select
        'SMT' as sec_org_desc,
        t1.loan_t2 as pu_name,
        sum(totals) as work_hour
    from lcfc.dwd_i_hr_iop_mh_d t1
    where t1.work_date >= '${first_day_of_month}'
      and t1.work_date <= '${now_1}'
      and t1.check_flag = '生效'
      and t1.iop_type in ('IOP')
      and t1.loan_t2 in ('PU3','PU4')
      and t1.loan_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
      and t1.loan_t4 in ('MTY')
    group by t1.loan_t2
    ) t3
  on t1.pu_name = t3.pu_name
;

    --SMT FRU、ECAT、R/S
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_smt_fru_hour_d_01
select
    '${first_day_of_month}' as stat_date,
    t1.sec_org_desc,
    --t1.thr_org_desc,
    --t1.four_org_desc,
    'FRU' as model_name,
    t1.pu_name,
    sum(t1.work_hour) as work_hour
from lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_01 t1
where (four_org_desc like '%FRU%' or four_org_desc like '%L6%')
  and pu_name in ('PU4','PU3')
group by t1.sec_org_desc,t1.pu_name
;
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_smt_fru_iop_hour_d_01
select
    '${first_day_of_month}' as stat_date,
    t1.sec_org_desc,
    'FRU' as model_name,
    t1.pu_name,
    t1.work_hour - coalesce(t2.work_hour,0) as work_hour
from (select
        'SMT' as sec_org_desc,
        t1.loan_t2 as pu_name,
        sum(t1.totals) as work_hour
    from lcfc.dwd_i_hr_iop_mh_d t1
    where t1.work_date >= '${first_day_of_month}'
      and t1.work_date <= '${now_1}'
      and t1.check_flag = '生效'
      and t1.iop_type in ('IOP')
      and upper(t1.borrow_t2) in ('FRU')
      and t1.loan_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
      and t1.loan_t2 in ('PU4','PU3')
    group by t1.loan_t2
    ) t1
left join (
    select
        'SMT' as sec_org_desc,
        t1.borrow_t2 as pu_name,
        sum(t1.totals) as work_hour
    from lcfc.dwd_i_hr_iop_mh_d t1
    where t1.work_date >= '${first_day_of_month}'
      and t1.work_date <= '${now_1}'
      and t1.check_flag = '生效'
      and t1.iop_type in ('IOP')
      and upper(t1.loan_t2) in ('FRU')
      and t1.loan_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
      and t1.borrow_t2 in ('PU4','PU3')
    group by t1.borrow_t2
    ) t2
  on t1.pu_name = t2.pu_name
;
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_smt_svc_iop_hour_d_01
select
    '${first_day_of_month}' as stat_date,
    'SVCFR' as model_name,
    t1.pu_name,
    t1.work_hour - coalesce(t2.work_hour,0) as work_hour
from (
    select
        'SMT' as sec_org_desc,
        t1.loan_t2 as pu_name,
        sum(t1.totals) as work_hour
    from lcfc.dwd_i_hr_iop_mh_d t1
    where t1.work_date >= '${first_day_of_month}'
      and t1.work_date <= '${now_1}'
      and t1.check_flag = '生效'
      and t1.iop_type in ('IOP')
      and upper(t1.borrow_t2) in ('DOA SERVICE')
      and t1.loan_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
      and t1.loan_t2 in ('PU4','PU3')
    group by t1.loan_t2
    ) t1
left join (
    select
        'SMT' as sec_org_desc,
        t1.borrow_t2 as pu_name,
        sum(t1.totals) as work_hour
    from lcfc.dwd_i_hr_iop_mh_d t1
    where t1.work_date >= '${first_day_of_month}'
      and t1.work_date <= '${now_1}'
      and t1.check_flag = '生效'
      and t1.iop_type in ('IOP')
      and upper(t1.loan_t2) in ('DOA SERVICE')
      and t1.loan_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
      and t1.borrow_t2 in ('PU4','PU3')
    group by t1.borrow_t2
    ) t2
  on t1.pu_name = t2.pu_name
;
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_smt_rs_hour_d_01
select
    '${first_day_of_month}' as stat_date,
    t1.pu_name,
    'R/S' as model_name,
    t1.work_hour - coalesce(t2.work_hour,0) as work_hour
from (
    select
        'SMT' as sec_org_desc,
        t1.borrow_t2 as pu_name,
        sum(totals) as work_hour
    from lcfc.dwd_i_hr_iop_mh_d t1
    where t1.work_date >= '${first_day_of_month}'
      and t1.work_date <= '${now_1}'
      and t1.check_flag = '生效'
      and t1.iop_type in ('IOP')
      and t1.borrow_t2 in ('PU3','PU4')
      and t1.borrow_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
      and t1.borrow_t4 in ('CCE')
    group by t1.borrow_t2
    ) t1
left join (
    select
        'SMT' as sec_org_desc,
        t1.loan_t2 as pu_name,
        sum(totals) as work_hour
    from lcfc.dwd_i_hr_iop_mh_d t1
    where t1.work_date >= '${first_day_of_month}'
      and t1.work_date <= '${now_1}'
      and t1.check_flag = '生效'
      and t1.iop_type in ('IOP')
      and t1.loan_t2 in ('PU3','PU4')
      and t1.loan_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
      and t1.loan_t4 in ('CCE')
    group by t1.loan_t2
    ) t2
  on t1.pu_name = t2.pu_name
;
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_smt_rs_hour_d_02
select
    '${first_day_of_month}' as stat_date,
    t1.pu_name,
    t1.process,
    'R/S' as model_name,
    t1.work_hour - coalesce(t2.work_hour,0) as work_hour
from (
    select
        'SMT' as sec_org_desc,
        t1.borrow_t2 as pu_name,
        t1.borrow_region as process,
        sum(totals) as work_hour
    from lcfc.dwd_i_hr_iop_mh_d t1
    where t1.work_date >= '${first_day_of_month}'
      and t1.work_date <= '${now_1}'
      and t1.check_flag = '生效'
      and t1.iop_type in ('IOP')
      and t1.borrow_t2 in ('PU3','PU4')
      and t1.borrow_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
      and t1.borrow_t4 in ('CCE')
    group by t1.borrow_t2,borrow_region
    ) t1
left join (
    select
        'SMT' as sec_org_desc,
        t1.loan_t2 as pu_name,
        t1.loan_region as process,
        sum(totals) as work_hour
    from lcfc.dwd_i_hr_iop_mh_d t1
    where t1.work_date >= '${first_day_of_month}'
      and t1.work_date <= '${now_1}'
      and t1.check_flag = '生效'
      and t1.iop_type in ('IOP')
      and t1.loan_t2 in ('PU3','PU4')
      and t1.loan_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
      and t1.loan_t4 in ('CCE')
    group by t1.loan_t2,loan_region
    ) t2
  on t1.pu_name = t2.pu_name
 and t1.process = t2.process
;
   -- FRU -> SVCFR
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_smt_fru_hour_d_02
select
    stat_date,
    sec_org_desc,
    'SVCFR' as model_name,
    t1.pu_name,
    t1.work_hour * 0.25 as work_hour
from (
    select
        t1.stat_date,
        t1.sec_org_desc,
        t1.model_name,
        t1.pu_name,
        t1.work_hour + coalesce(t2.work_hour,0) as work_hour
    from lcfc.tmp_dwa_i_mfg_attend_smt_fru_hour_d_01 t1
    left join lcfc.tmp_dwa_i_mfg_attend_smt_fru_iop_hour_d_01 t2
      on t1.pu_name = t2.pu_name
    where t1.pu_name = 'PU4'
    ) t1
union all
select
    stat_date,
    sec_org_desc,
    'ECAT' as model_name,
    t1.pu_name,
    t1.work_hour * 0.5 as work_hour
from (
    select
        t1.stat_date,
        t1.sec_org_desc,
        t1.model_name,
        t1.pu_name,
        t1.work_hour + coalesce(t2.work_hour,0) as work_hour
    from lcfc.tmp_dwa_i_mfg_attend_smt_fru_hour_d_01 t1
    left join lcfc.tmp_dwa_i_mfg_attend_smt_fru_iop_hour_d_01 t2
      on t1.pu_name = t2.pu_name
    where t1.pu_name = 'PU4'
    ) t1
union all
select
    stat_date,
    sec_org_desc,
    'R/S' as model_name,
    t1.pu_name,
    t1.work_hour * 0.25 as work_hour
from (
    select
        t1.stat_date,
        t1.sec_org_desc,
        t1.model_name,
        t1.pu_name,
        t1.work_hour + coalesce(t2.work_hour,0) as work_hour
    from lcfc.tmp_dwa_i_mfg_attend_smt_fru_hour_d_01 t1
    left join lcfc.tmp_dwa_i_mfg_attend_smt_fru_iop_hour_d_01 t2
      on t1.pu_name = t2.pu_name
    where t1.pu_name = 'PU4'
    ) t1
union all
select
    t1.stat_date,
    t1.sec_org_desc,
    'SVCFR' model_name,
    t1.pu_name,
    t1.work_hour + coalesce(t2.work_hour,0) as work_hour
from lcfc.tmp_dwa_i_mfg_attend_smt_fru_hour_d_01 t1
left join lcfc.tmp_dwa_i_mfg_attend_smt_fru_iop_hour_d_01 t2
  on t1.pu_name = t2.pu_name
where t1.pu_name = 'PU3'
;
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_smt_mty_option_hour_d_01
select
    '${first_day_of_month}' as stat_date,
    t1.model_name,
    'SMT' as sec_org_desc,
    t1.pu_name,
    sum(work_hour) as work_hour
from (
    select
        'BOX' as sec_org_desc,
        'R/S' as model_name,
        t1.borrow_t2 as pu_name,
        sum(totals) as work_hour
    from lcfc.dwd_i_hr_iop_mh_d t1
    where t1.work_date >= '${first_day_of_month}'
      and t1.work_date <= '${now_1}'
      and t1.check_flag = '生效'
      and t1.iop_type in ('IOP')
      and t1.borrow_t2 in ('PU3','PU4')
      and t1.borrow_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
      and upper(t1.borrow_region) in ('OPTION')
	  and upper(t1.borrow_t2) not in ('FRU')
    group by borrow_t2
    union all
    select
        'BOX' as sec_org_desc,
        'R/S' as model_name,
        t1.loan_t2 as pu_name,
        0 - sum(totals) as work_hour
    from lcfc.dwd_i_hr_iop_mh_d t1
    where t1.work_date >= '${first_day_of_month}'
      and t1.work_date <= '${now_1}'
      and t1.check_flag = '生效'
      and t1.iop_type in ('IOP')
      and t1.loan_t2 in ('PU3','PU4')
      and t1.loan_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
      and upper(t1.loan_region) in ('OPTION')
	  and upper(t1.loan_t2) not in ('FRU')
    group by loan_t2
    ) t1
	group by t1.model_name,
    t1.pu_name
;
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_smt_m_hour_d_01
select
    stat_date,
    sec_org_desc,
    model_name,
    pu_name,
    sum(work_hour) work_hour
from (
    select
        stat_date,
        sec_org_desc,
        model_name,
        pu_name,
        work_hour
    from lcfc.tmp_dwa_i_mfg_attend_smt_mty_hour_d_01  --MTY (PU3、PU4)
    union all
    select
        stat_date,
        sec_org_desc,
        model_name,
        pu_name,
        work_hour
    from lcfc.tmp_dwa_i_mfg_attend_smt_fru_hour_d_02 --fru (PU4,PU3)
    union all
    select
        stat_date,
        'SMT' sec_org_desc,
        model_name,
        pu_name,
        work_hour
    from lcfc.tmp_dwa_i_mfg_attend_smt_svc_iop_hour_d_01  --SVC (DOA service -PU3、PU4)
    union all
    select
        stat_date,
        'SMT' sec_org_desc,
        model_name,
        pu_name,
        work_hour
    from lcfc.tmp_dwa_i_mfg_attend_smt_rs_hour_d_01  --R/S(PU3、PU4)
	union all
	select
		stat_date,
		'SMT' sec_org_desc,
        model_name,
        pu_name,
        work_hour
	from lcfc.tmp_dwa_i_mfg_attend_smt_mty_option_hour_d_01  --R/S(PU3、PU4)
    ) t
group by stat_date,
    sec_org_desc,
    model_name,
    pu_name
;
    --BOX R/S、Service、MTY、FRU
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_box_mty_hour_d_01
select
    '${first_day_of_month}' as stat_date,
    case when four_org_desc like '%MTY%' then 'MTY'
         when four_org_desc like '%Option%' then 'R/S'
         when four_org_desc like '%Service%' then 'SVCFR'
         when four_org_desc like '%Remote Site%' then 'R/S'
    end as model_name,
    t1.sec_org_desc,
    t1.pu_name,
    sum(t1.work_hour) as work_hour
from lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_01 t1
where (four_org_desc  like '%MTY%' or four_org_desc like '%Option%' or four_org_desc like '%Service%' or four_org_desc like '%Remote Site%')
  and pu_name in ('PU1','PU2')
group by case when four_org_desc like '%MTY%' then 'MTY'
         when four_org_desc like '%Option%' then 'R/S'
         when four_org_desc like '%Service%' then 'SVCFR'
         when four_org_desc like '%Remote Site%' then 'R/S'
    end,
    t1.sec_org_desc,
    t1.pu_name
;
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_box_mty_iop_hour_d_01
select
    '${first_day_of_month}' as stat_date,
    t1.model_name,
    'BOX' as sec_org_desc,
    t1.pu_name,
    t1.work_hour - coalesce(t2.work_hour,0) as work_hour
from (
    select
        'BOX' as sec_org_desc,
        case when upper(borrow_t4) in ('MTY') then 'MTY' else '' end as model_name,
        t1.borrow_t2 as pu_name,
        sum(totals) as work_hour
    from lcfc.dwd_i_hr_iop_mh_d t1
    where t1.work_date >= '${first_day_of_month}'
      and t1.work_date <= '${now_1}'
      and t1.check_flag = '生效'
      and t1.iop_type in ('IOP')
      and t1.borrow_t2 in ('PU1','PU2')
      and t1.borrow_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
      and upper(t1.borrow_t4) in ('MTY')
    group by case when upper(borrow_t4) in ('MTY') then 'MTY' else '' end,borrow_t2
    ) t1
left join (
    select
        'BOX' as sec_org_desc,
        case when upper(loan_t4) in ('MTY') then 'MTY' else '' end as model_name,
        t1.loan_t2 as pu_name,
        sum(totals) as work_hour
    from lcfc.dwd_i_hr_iop_mh_d t1
    where t1.work_date >= '${first_day_of_month}'
      and t1.work_date <= '${now_1}'
      and t1.check_flag = '生效'
      and t1.iop_type in ('IOP')
      and t1.loan_t2 in ('PU1','PU2')
      and t1.loan_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
      and upper(t1.loan_t4) in ('MTY')
    group by case when upper(loan_t4) in ('MTY') then 'MTY' else '' end,loan_t2
    ) t2
  on t1.pu_name = t2.pu_name
 and t1.model_name = t2.model_name
;
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_box_mty_option_hour_d_01
select
    '${first_day_of_month}' as stat_date,
    t1.model_name,
    'BOX' as sec_org_desc,
    t1.pu_name,
    sum(work_hour) as work_hour
from (
    select
        'BOX' as sec_org_desc,
        'R/S' as model_name,
        t1.borrow_t2 as pu_name,
        sum(totals) as work_hour
    from lcfc.dwd_i_hr_iop_mh_d t1
    where t1.work_date >= '${first_day_of_month}'
      and t1.work_date <= '${now_1}'
      and t1.check_flag = '生效'
      and t1.iop_type in ('IOP')
      and t1.borrow_t2 in ('PU1','PU2')
      and t1.borrow_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
      and upper(t1.borrow_region) in ('OPTION')
	  and upper(t1.borrow_t2) not in ('FRU')
    group by borrow_t2
    union all
	select
        'BOX' as sec_org_desc,
        'R/S' as model_name,
        t1.loan_t2 as pu_name,
        0 - sum(totals) as work_hour
    from lcfc.dwd_i_hr_iop_mh_d t1
    where t1.work_date >= '${first_day_of_month}'
      and t1.work_date <= '${now_1}'
      and t1.check_flag = '生效'
      and t1.iop_type in ('IOP')
      and t1.loan_t2 in ('PU1','PU2')
      and t1.loan_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
      and upper(t1.loan_region) in ('OPTION')
	  and upper(t1.loan_t2) not in ('FRU')
    group by loan_t2
    ) t1
	group by t1.model_name,
    t1.pu_name
;

insert overwrite table lcfc.tmp_dwa_i_mfg_attend_box_svc_iop_hour_d_01
select
    '${first_day_of_month}' as stat_date,
    'SVCFR' as model_name,
    t1.pu_name,
    t1.work_hour - coalesce(t2.work_hour,0) as work_hour
from (
    select
        'BOX' as sec_org_desc,
        t1.loan_t2 as pu_name,
        sum(t1.totals) as work_hour
    from lcfc.dwd_i_hr_iop_mh_d t1
    where t1.work_date >= '${first_day_of_month}'
      and t1.work_date <= '${now_1}'
      and t1.check_flag = '生效'
      and t1.iop_type in ('IOP')
      and upper(t1.borrow_t2) in ('DOA SERVICE','FRU')
      and t1.loan_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
      and t1.loan_t2 in ('PU1','PU2')
    group by t1.loan_t2
    ) t1
left join (
    select
        'BOX' as sec_org_desc,
        t1.borrow_t2 as pu_name,
        sum(t1.totals) as work_hour
    from lcfc.dwd_i_hr_iop_mh_d t1
    where t1.work_date >= '${first_day_of_month}'
      and t1.work_date <= '${now_1}'
      and t1.check_flag = '生效'
      and t1.iop_type in ('IOP')
      and upper(t1.loan_t2) in ('DOA SERVICE','FRU')
      and t1.loan_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
      and t1.borrow_t2 in ('PU1','PU2')
    group by t1.borrow_t2
    ) t2
  on t1.pu_name = t2.pu_name
;
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_box_m_hour_d_01
select
    stat_date,
    model_name,
    pu_name,
    sec_org_desc,
    sum(work_hour) as work_hour
from (
    select
        stat_date,
        model_name,
        pu_name,
        sec_org_desc,
        work_hour
    from lcfc.tmp_dwa_i_mfg_attend_box_mty_hour_d_01 t1
    union all
    select
        stat_date,
        model_name,
        pu_name,
        sec_org_desc,
        work_hour
    from lcfc.tmp_dwa_i_mfg_attend_box_mty_iop_hour_d_01 t2
    union all
    select
        stat_date,
        model_name,
        pu_name,
        'BOX' sec_org_desc,
        work_hour
    from lcfc.tmp_dwa_i_mfg_attend_box_svc_iop_hour_d_01 t3
	union all
	select
		stat_date,
        model_name,
        pu_name,
        'BOX' sec_org_desc,
        work_hour
	from lcfc.tmp_dwa_i_mfg_attend_box_mty_option_hour_d_01 t4
    ) t
group by stat_date,
        model_name,
        pu_name,
        sec_org_desc
;
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_mty_hour_d_03
select
    stat_date,
    model_name,
    sec_org_desc,
    pu_name,
    'MP' as phase,
    'SUB' process,
    work_hour
from lcfc.tmp_dwa_i_mfg_attend_box_m_hour_d_01
union all
select
    stat_date,
    model_name,
    sec_org_desc,
    pu_name,
    'MP' as phase,
    'SMT' process,
    work_hour
from lcfc.tmp_dwa_i_mfg_attend_smt_m_hour_d_01

-- 7. DT（桌面设备）工时处理
/**
  来源表：
  lcfc.tmp_dwa_i_mfg_attend_iop_hour_d_01
  代码逻辑：
  1. 拆分多模型名称字段（如 / 分隔），并对借出和借入工时进行差值计算
  2. 按照模型、部门、PU、阶段等字段进行平均分配。
  输出表：
  lcfc.tmp_dwa_i_mfg_attend_dt_hour_d_01
  lcfc.tmp_dwa_i_mfg_attend_dt_hour_d_02
 */
-- DT工时1
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_dt_hour_d_01
select
    stat_date,
    model_name1,
    model_name,
    sec_org_desc,
    pu_name,
    phase,
    coalesce(process,'-1') as process,
    loan_hour,
    borrow_hour,
    count(*) as num
from (
    select
        stat_date,
        model_name,
        model_name1,
        sec_org_desc,
        pu_name,
        phase,
        process,
        loan_hour,
        borrow_hour
    from(
        select
            stat_date,
            model_name,
            sec_org_desc,
            pu_name,
            phase,
            process,
            sum(loan_hour) as loan_hour,
            sum(borrow_hour) as borrow_hour
        from (
            select
                stat_date,
                model_name as model_name,
                sec_org_desc,
                pu_name,
                'MP' as phase,
                case when sec_org_desc = 'BOX' and group_process = 'Others' then 'SUB'
                     when sec_org_desc = 'SMT' and group_process = 'Others' then 'SMT'
					 else group_process
				end as process,
                coalesce(t1.loan_hour,0) loan_hour,
                coalesce(t1.borrow_hour,0) borrow_hour
            from lcfc.tmp_dwa_i_mfg_attend_iop_hour_d_01 t1
            where substr(t4_org_desc,1,3) = 'DT_'
              and model_name is not null
              and trim(model_name) <> ''
              and instr(model_name,'&') = 0
              and borrow_hour <> 0
            union all
            select
                stat_date,
                model_name as model_name,
                sec_org_desc,
                pu_name,
                'MP' as phase,
                case when sec_org_desc = 'BOX' and group_process = 'Others' then 'SUB'
                     when sec_org_desc = 'SMT' and group_process = 'Others' then 'SMT'
					 else group_process
				end as process,
                coalesce(t1.loan_hour,0) loan_hour,
                coalesce(t1.borrow_hour,0) borrow_hour
            from lcfc.tmp_dwa_i_mfg_attend_iop_hour_d_01 t1
            where substr(t4_org_desc,1,3) = 'DT_'
              and model_name is not null
              and trim(model_name) <> ''
              and instr(model_name,'&') = 0
              and borrow_hour = 0
            ) t
        group by stat_date,
            model_name,
            sec_org_desc,
            pu_name,
            phase,
            process
        ) t
    lateral view explode(split(model_name, '/')) tmp as model_name1
    ) t
group by stat_date,
    model_name1,
    model_name,
    sec_org_desc,
    pu_name,
    phase,
    coalesce(process,'-1'),
    loan_hour,
    borrow_hour

-- DT工时
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_dt_hour_d_02
select
    t1.stat_date,
    t1.model_name1 as model_name,
    t1.sec_org_desc,
    t1.pu_name,
    t1.phase,
    t1.process,
    sum((coalesce(t1.borrow_hour,0) - coalesce(t1.loan_hour,0))/t2.num) as work_hour
from lcfc.tmp_dwa_i_mfg_attend_dt_hour_d_01 t1
left join (
    select
        stat_date,
        model_name,
        sec_org_desc,
        pu_name,
        phase,
        coalesce(process,'-1') as process,
        count(model_name1) as num
    from lcfc.tmp_dwa_i_mfg_attend_dt_hour_d_01
    group by stat_date,
        model_name,
        sec_org_desc,
        pu_name,
        phase,
        coalesce(process,'-1')
    ) t2
  on t1.stat_date = t2.stat_date
 and t1.model_name = t2.model_name
 and t1.sec_org_desc =t2.sec_org_desc
 and t1.phase = t2.phase
 and t1.process = t2.process
 and t1.pu_name = t2.pu_name
group by t1.stat_date,
    t1.model_name1,
    t1.sec_org_desc,
    t1.pu_name,
    t1.phase,
    t1.process

-- 8. NPI（新产品导入）工时处理
/**
  来源表：
  lcfc.dwd_i_hr_iop_mh_d
  代码逻辑：
   1. 过滤非 MP 阶段的 NPI 工时
   2. 拆分多模型名称字段，并对借出和借入工时进行差值计算
  输出表：
  lcfc.tmp_dwa_i_mfg_attend_npi_hour_d_01
  lcfc.tmp_dwa_i_mfg_attend_npi_hour_d_02
  lcfc.tmp_dwa_i_mfg_attend_npi_hour_d_03
 */
-- NPI工时1
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_npi_hour_d_01
select
    stat_date,
    sec_org_desc,
    pu_name,
    '' t4_org_desc,
    group_process,
    model_name,
    sum(loan_hour) as loan_hour,
    sum(borrow_hour) as borrow_hour
from (
    select
        stat_date,
        model_name,
        sec_org_desc,
        pu_name,
        case when group_process = 'OTHERS' and sec_org_desc = 'BOX' then 'ASSY' when group_process = 'OTHERS' and sec_org_desc = 'SMT' then 'SMT' else group_process end as group_process,
        coalesce(t1.loan_hour,0) loan_hour,
        coalesce(t1.borrow_hour,0) borrow_hour
    from (
        select
            from_unixtime(unix_timestamp(work_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd')  as stat_date,
            case when loan_t2 in ('PU1','PU2') then 'BOX'
                 when loan_t2 in ('PU3','PU4') then 'SMT'
            end as sec_org_desc,
            loan_t2 as pu_name,
            loan_t4 as t4_org_desc,
            case when loan_region = 'PCB' then 'PCBA' else upper(loan_region) end as group_process,
            model_name,
            0 - totals as loan_hour,
            0 as borrow_hour
        from lcfc.dwd_i_hr_iop_mh_d t1
        where t1.work_date >= '${first_day_of_month}'
          and t1.work_date <= '${now_1}'
          and t1.check_flag = '生效'
          and t1.iop_type in ('NPI')
          and t1.loan_t2 in ('PU1','PU2','PU3','PU4')
          and coalesce(t1.phase1,'-999') not in ('SVT','SOVP','MP')
        union all
        select
            from_unixtime(unix_timestamp(work_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd')  as stat_date,
            case when borrow_t2 in ('PU1','PU2') then 'BOX'
                 when borrow_t2 in ('PU3','PU4') then 'SMT'
            end as sec_org_desc,
            borrow_t4 as t4_org_desc,
            borrow_t2 as pu_name,
            case when loan_region = 'PCB' then 'PCBA' else upper(loan_region) end as group_process,
            model_name,
            0 as loan_hour,
            totals - 0 as borrow_hour
        from lcfc.dwd_i_hr_iop_mh_d t1
        where t1.work_date >= '${first_day_of_month}'
          and t1.work_date <= '${now_1}'
          and t1.check_flag = '生效'
          and t1.iop_type in ('NPI')
          and t1.borrow_t2 in ('PU1','PU2','PU3','PU4')
          and coalesce(t1.phase1,'-999') not in ('SVT','SOVP','MP')
        ) t1
    ) t
group by stat_date,
    sec_org_desc,
    pu_name,
    group_process,
    model_name

-- NPI工时
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_npi_hour_d_02
select
    stat_date,
    model_name1,
    model_name,
    sec_org_desc,
    pu_name,
    phase,
    process,
    loan_hour,
    borrow_hour,
    count(*) as num
from (
    select
        stat_date,
        model_name,
        model_name1 as model_name1,
        sec_org_desc,
        pu_name,
        'NPI' as phase,
        case when group_process = 'OTHERS' and sec_org_desc = 'BOX' then 'ASSY' when group_process = 'OTHERS' and sec_org_desc = 'SMT' then 'SMT' else group_process end as process,
        coalesce(t1.loan_hour,0) loan_hour,
        coalesce(t1.borrow_hour,0) borrow_hour
    from lcfc.tmp_dwa_i_mfg_attend_npi_hour_d_01 t1
    lateral view explode(split(model_name, '/')) tmp as model_name1
    ) t
group by stat_date,
    model_name1,
    model_name,
    sec_org_desc,
    pu_name,
    phase,
    process,
    loan_hour,
    borrow_hour
;

-- NPI结果
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_npi_hour_d_03
select
    t1.stat_date,
    t1.model_name1 as model_name,
    t1.sec_org_desc,
    t1.pu_name,
    t1.phase,
    t1.process,
    sum((coalesce(t1.loan_hour,0) - coalesce(t1.borrow_hour,0))/coalesce(t2.num,0)) as work_hour
from lcfc.tmp_dwa_i_mfg_attend_npi_hour_d_02 t1
left join (
    select
        t1.stat_date,
        t1.model_name,
        t1.sec_org_desc,
        t1.pu_name,
        t1.phase,
        t1.process,
        count(model_name1) as num
    from lcfc.tmp_dwa_i_mfg_attend_npi_hour_d_02 t1
    group by t1.stat_date,
        t1.model_name,
        t1.sec_org_desc,
        t1.pu_name,
        t1.phase,
        t1.process
    ) t2
  on t1.stat_date = t2.stat_date
 and t1.model_name = t2.model_name
 and t1.sec_org_desc = t2.sec_org_desc
 and t1.pu_name = t2.pu_name
 and t1.phase = t2.phase
 and t1.process = t2.process
group by t1.stat_date,
    t1.model_name1,
    t1.sec_org_desc,
    t1.pu_name,
    t1.phase,
    t1.process

-- 9. Rework（返工）工时处理
/**
  来源表：
  lcfc.dwd_i_hr_iop_mh_d
  代码逻辑：
  统计返工工时，并按照部门、PU、阶段等字段进行拆分和平均分配
  输出表：
  lcfc.tmp_dwa_i_mfg_attend_rework_hour_d_01
  lcfc.tmp_dwa_i_mfg_attend_rework_hour_d_02
 */
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_rework_hour_d_01
select
    stat_date,
    model_name1,
    model_name,
    sec_org_desc,
    pu_name,
    phase,
    coalesce(process,'-1') as process,
    loan_hour,
    borrow_hour,
    count(*) as num
from (
    select
        stat_date,
        model_name,
        model_name1,
        sec_org_desc,
        pu_name,
        phase,
        process,
        loan_hour,
        borrow_hour
    from (
        select
            stat_date,
            model_name,
            sec_org_desc,
            pu_name,
            phase,
            process,
            sum(loan_hour) as loan_hour,
            sum(borrow_hour) as borrow_hour
        from (
            select
                from_unixtime(unix_timestamp(work_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd')  as stat_date,
                case when loan_t2 in ('PU1','PU2') then 'BOX'
                     when loan_t2 in ('PU3','PU4') then 'SMT'
                end as sec_org_desc,
                loan_t2 as pu_name,
                loan_t3 as t3_org_name,
                loan_t4 as t4_org_desc,
                case when loan_region = 'PCB' then 'PCB' when loan_region = 'Option' then 'SUB' else loan_region end as process,
                coalesce(model_name,'') as model_name,
                totals as loan_hour,
                0 as borrow_hour,
                phase1 as phase
            from lcfc.dwd_i_hr_iop_mh_d t1
            where t1.work_date >= '${first_day_of_month}'
              and t1.work_date <= '${now_1}'
              and t1.check_flag = '生效'
              and t1.iop_type in ('REWORK')
              and t1.loan_t2 in ('PU1','PU2','PU3','PU4')
              and t1.loan_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
            union all
            select
                from_unixtime(unix_timestamp(work_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd')  as stat_date,
                case when borrow_t2 in ('PU1','PU2') then 'BOX'
                     when borrow_t2 in ('PU3','PU4') then 'SMT'
                end as sec_org_desc,
                borrow_t2 as pu_name,
                borrow_t3 as t3_org_name,
                borrow_t4 as t4_org_desc,
                case when borrow_region = 'PCB' then 'PCBA' when borrow_region = 'Option' then 'SUB' else borrow_region end as process,
                coalesce(model_name,'') as model_name,
                0 as loan_hour,
                totals as borrow_hour,
                phase1 as phase
            from lcfc.dwd_i_hr_iop_mh_d t1
            where t1.work_date >= '${first_day_of_month}'
              and t1.work_date <= '${now_1}'
              and t1.check_flag = '生效'
              and t1.iop_type in ('REWORK')
              and t1.borrow_t2 in ('PU1','PU2','PU3','PU4')
              and t1.borrow_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
            ) t1
        group by stat_date,
            model_name,
            sec_org_desc,
            pu_name,
            phase,
            process
        ) t
        lateral view explode(split(model_name, '/')) tmp as model_name1
    )t
group by stat_date,
    model_name1,
    model_name,
    sec_org_desc,
    pu_name,
    phase,
    coalesce(process,'-1'),
    loan_hour,
    borrow_hour
;
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_rework_hour_d_02
select
    t1.stat_date,
    t1.model_name1 as model_name,
    t1.sec_org_desc,
    t1.pu_name,
    'MP' as phase,
    case when t1.sec_org_desc = 'BOX' then 'SUB'
         when t1.sec_org_desc = 'SMT' then 'SMT'
    end as process,
    sum((coalesce(t1.borrow_hour,0) - coalesce(t1.loan_hour,0))/t2.num) as work_hour
from lcfc.tmp_dwa_i_mfg_attend_rework_hour_d_01 t1
left join (
    select
        stat_date,
        model_name,
        sec_org_desc,
        pu_name,
        phase,
        coalesce(process,'-1') as process,
        count(model_name1) as num
    from lcfc.tmp_dwa_i_mfg_attend_rework_hour_d_01
    group by stat_date,
        model_name,
        sec_org_desc,
        pu_name,
        phase,
        coalesce(process,'-1')
    ) t2
  on t1.stat_date = t2.stat_date
 and t1.model_name = t2.model_name
 and t1.sec_org_desc =t2.sec_org_desc
 and t1.phase = t2.phase
 and t1.process = t2.process
 and t1.pu_name = t2.pu_name
group by t1.stat_date,
    t1.model_name1,
    t1.sec_org_desc,
    t1.pu_name,
    case when t1.sec_org_desc = 'BOX' then 'SUB'
         when t1.sec_org_desc = 'SMT' then 'SMT'
    end

-- 10.SSD/Lark/Medion/CB/NEC 特殊产品工时处理
/**
  来源表：
  lcfc.dwd_i_hr_iop_mh_d
  代码逻辑：
   1. 分别统计 SSD、Lark、Medion、CB、NEC 等产品的工时
   2. 拆分多模型名称字段，并进行平均分配
  输出表：
  lcfc.tmp_dwa_i_mfg_attend_ssd_hour_d_01, lcfc.tmp_dwa_i_mfg_attend_ssd_hour_d_02
  lcfc.tmp_dwa_i_mfg_attend_lark_hour_d_01, lcfc.tmp_dwa_i_mfg_attend_lark_hour_d_02
  lcfc.tmp_dwa_i_mfg_attend_medion_hour_d_01, lcfc.tmp_dwa_i_mfg_attend_medion_hour_d_02
  lcfc.tmp_dwa_i_mfg_attend_cb_hour_d_01
  lcfc.tmp_dwa_i_mfg_attend_nec_hour_d_01
 */

-- SSD_Lark
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_ssd_hour_d_01
select
    stat_date,
    model_name1,
    model_name,
    sec_org_desc,
    pu_name,
    phase,
    coalesce(process,'-1') as process,
	work_hour,
    count(*) as num
from (
    select
        stat_date,
        model_name,
        model_name1,
        sec_org_desc,
        pu_name,
        phase,
        process,
		work_hour
    from(
        select
            stat_date,
            model_name,
            sec_org_desc,
            pu_name,
            phase,
            process,
            sum(work_hour) as work_hour
        from (
                select
					from_unixtime(unix_timestamp(work_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd')  as stat_date,
					case when loan_t2 in ('PU1','PU2') then 'BOX'
						 when loan_t2 in ('PU3','PU4') then 'SMT'
					end as sec_org_desc,
					loan_t2 as pu_name,
					loan_t3 as t3_org_name,
					loan_t4 as t4_org_desc,
					case when loan_region = 'PCB' then 'PCBA' when loan_region = 'Option' then 'SUB' else loan_region end as process,
					coalesce(model_name,'') as model_name,
					'MP' as phase,
                    t1.totals as work_hour
                from lcfc.dwd_i_hr_iop_mh_d t1
                where t1.work_date >= '${first_day_of_month}'
                  and t1.work_date < '${now}'
                  and t1.check_flag = '生效'
                  and t1.iop_type in ('IOP','REWORK')
                  and substr(t1.borrow_t4,1,3) = 'SSD'
                  and t1.loan_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
                  and t1.loan_t2 in ('PU4','PU3','PU1','PU2')
            ) t
        group by stat_date,
            model_name,
            sec_org_desc,
            pu_name,
            phase,
            process
        ) t
    lateral view explode(split(model_name, '/')) tmp as model_name1
    ) t
group by stat_date,
    model_name1,
    model_name,
    sec_org_desc,
    pu_name,
    phase,
    coalesce(process,'-1'),
    work_hour;
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_ssd_hour_d_02
select
    t1.stat_date,
    t1.model_name1 as model_name,
    t1.sec_org_desc,
    t1.pu_name,
    t1.phase,
    t1.process,
    sum(coalesce(t1.work_hour,0)/t2.num) as work_hour
from lcfc.tmp_dwa_i_mfg_attend_ssd_hour_d_01 t1
left join (
    select
        stat_date,
        model_name,
        sec_org_desc,
        pu_name,
        phase,
        coalesce(process,'-1') as process,
        count(model_name1) as num
    from lcfc.tmp_dwa_i_mfg_attend_ssd_hour_d_01
    group by stat_date,
        model_name,
        sec_org_desc,
        pu_name,
        phase,
        coalesce(process,'-1')
    ) t2
  on t1.stat_date = t2.stat_date
 and t1.model_name = t2.model_name
 and t1.sec_org_desc =t2.sec_org_desc
 and t1.phase = t2.phase
 and t1.process = t2.process
 and t1.pu_name = t2.pu_name
group by t1.stat_date,
    t1.model_name1,
    t1.sec_org_desc,
    t1.pu_name,
    t1.phase,
    t1.process;
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_lark_hour_d_01
select
    stat_date,
    model_name1,
    model_name,
    sec_org_desc,
    pu_name,
    phase,
    coalesce(process,'-1') as process,
	work_hour,
    count(*) as num
from (
    select
        stat_date,
        model_name,
        model_name1,
        sec_org_desc,
        pu_name,
        phase,
        process,
		work_hour
    from(
        select
            stat_date,
            model_name,
            sec_org_desc,
            pu_name,
            phase,
            process,
            sum(work_hour) as work_hour
        from (
                select
					from_unixtime(unix_timestamp(work_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd')  as stat_date,
					case when loan_t2 in ('PU1','PU2') then 'BOX'
						 when loan_t2 in ('PU3','PU4') then 'SMT'
					end as sec_org_desc,
					loan_t2 as pu_name,
					loan_t3 as t3_org_name,
					loan_t4 as t4_org_desc,
					case when loan_region = 'PCB' then 'PCBA' when loan_region = 'Option' then 'SUB' else loan_region end as process,
					coalesce(model_name,'') as model_name,
					'MP' as phase,
                    t1.totals as work_hour
                from lcfc.dwd_i_hr_iop_mh_d t1
                where t1.work_date >= '${first_day_of_month}'
                  and t1.work_date <= '${now_1}'
                  and t1.check_flag = '生效'
                  and t1.iop_type in ('IOP','REWORK')
                  and t1.borrow_t4 = 'Lark'
                  and t1.loan_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
                  and t1.loan_t2 in ('PU4','PU3','PU1','PU2')
            ) t
        group by stat_date,
            model_name,
            sec_org_desc,
            pu_name,
            phase,
            process
        ) t
    lateral view explode(split(model_name, '/')) tmp as model_name1
    ) t
group by stat_date,
    model_name1,
    model_name,
    sec_org_desc,
    pu_name,
    phase,
    coalesce(process,'-1'),
    work_hour;
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_lark_hour_d_02
select
    t1.stat_date,
    t1.model_name1 as model_name,
    t1.sec_org_desc,
    t1.pu_name,
    t1.phase,
    t1.process,
    sum(coalesce(t1.work_hour,0)/t2.num) as work_hour
from lcfc.tmp_dwa_i_mfg_attend_lark_hour_d_01 t1
left join (
    select
        stat_date,
        model_name,
        sec_org_desc,
        pu_name,
        phase,
        coalesce(process,'-1') as process,
        count(model_name1) as num
    from lcfc.tmp_dwa_i_mfg_attend_lark_hour_d_01
    group by stat_date,
        model_name,
        sec_org_desc,
        pu_name,
        phase,
        coalesce(process,'-1')
    ) t2
  on t1.stat_date = t2.stat_date
 and t1.model_name = t2.model_name
 and t1.sec_org_desc =t2.sec_org_desc
 and t1.phase = t2.phase
 and t1.process = t2.process
 and t1.pu_name = t2.pu_name
group by t1.stat_date,
    t1.model_name1,
    t1.sec_org_desc,
    t1.pu_name,
    t1.phase,
    t1.process;
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_medion_hour_d_01
select
    stat_date,
    model_name1,
    model_name,
    sec_org_desc,
    pu_name,
    phase,
    coalesce(process,'-1') as process,
	work_hour,
    count(*) as num
from (
    select
        stat_date,
        model_name,
        model_name1,
        sec_org_desc,
        pu_name,
        phase,
        process,
		work_hour
    from(
        select
            stat_date,
            model_name,
            sec_org_desc,
            pu_name,
            phase,
            process,
            sum(work_hour) as work_hour
        from (
                select
					from_unixtime(unix_timestamp(work_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd')  as stat_date,
					case when loan_t2 in ('PU1','PU2') then 'BOX'
						 when loan_t2 in ('PU3','PU4') then 'SMT'
					end as sec_org_desc,
					loan_t2 as pu_name,
					loan_t3 as t3_org_name,
					loan_t4 as t4_org_desc,
					case when loan_region = 'PCB' then 'PCBA' when loan_region = 'Option' then 'SUB' else loan_region end as process,
					coalesce(model_name,'') as model_name,
					'MP' as phase,
                    t1.totals as work_hour
                from lcfc.dwd_i_hr_iop_mh_d t1
                where t1.work_date >= '${first_day_of_month}'
				  and t1.work_date <= '${now_1}'
				  and t1.check_flag = '生效'
				  and t1.iop_type in ('IOP','REWORK')
				  and substr(t1.borrow_t4,1,6) = 'Medion'
				  and t1.loan_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
				  and t1.loan_t2 in ('PU4','PU3','PU1','PU2')
            ) t
        group by stat_date,
            model_name,
            sec_org_desc,
            pu_name,
            phase,
            process
        ) t
    lateral view explode(split(model_name, '/')) tmp as model_name1
    ) t
group by stat_date,
    model_name1,
    model_name,
    sec_org_desc,
    pu_name,
    phase,
    coalesce(process,'-1'),
    work_hour;
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_medion_hour_d_02
select
    t1.stat_date,
    t1.model_name1 as model_name,
    t1.sec_org_desc,
    t1.pu_name,
    t1.phase,
    t1.process,
    sum(coalesce(t1.work_hour,0)/t2.num) as work_hour
from lcfc.tmp_dwa_i_mfg_attend_medion_hour_d_01 t1
left join (
    select
        stat_date,
        model_name,
        sec_org_desc,
        pu_name,
        phase,
        coalesce(process,'-1') as process,
        count(model_name1) as num
    from lcfc.tmp_dwa_i_mfg_attend_medion_hour_d_01
    group by stat_date,
        model_name,
        sec_org_desc,
        pu_name,
        phase,
        coalesce(process,'-1')
    ) t2
  on t1.stat_date = t2.stat_date
 and t1.model_name = t2.model_name
 and t1.sec_org_desc =t2.sec_org_desc
 and t1.phase = t2.phase
 and t1.process = t2.process
 and t1.pu_name = t2.pu_name
group by t1.stat_date,
    t1.model_name1,
    t1.sec_org_desc,
    t1.pu_name,
    t1.phase,
    t1.process;

-- CB_NEC
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_cb_hour_d_01
select
	stat_date,
	'CB' model_name,
	sec_org_desc,
	pu_name,
	phase,
	process,
	sum(work_hour) as work_hour
from (
	select
		from_unixtime(unix_timestamp(work_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd')  as stat_date,
		case when loan_t2 in ('PU1','PU2') then 'BOX'
			 when loan_t2 in ('PU3','PU4') then 'SMT'
		end as sec_org_desc,
		loan_t2 as pu_name,
		loan_t3 as t3_org_name,
		loan_t4 as t4_org_desc,
		case when loan_region = 'PCB' then 'PCB' when loan_region = 'Option' then 'SUB' else loan_region end as process,
		coalesce(model_name,'') as model_name,
		'MP' as phase,
		t1.totals as work_hour
	from lcfc.dwd_i_hr_iop_mh_d t1
	where t1.work_date >= '${first_day_of_month}'
	  and t1.work_date <= '${now_1}'
	  and t1.check_flag = '生效'
	  and t1.iop_type in ('IOP','REWORK')
	  and substr(t1.borrow_t4,1,2) = 'CB'
	  and t1.loan_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
	  and t1.loan_t2 in ('PU4','PU3','PU1','PU2')
	) t1
	group by stat_date,
	--model_name,
	sec_org_desc,
	pu_name,
	phase,
	process;
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_nec_hour_d_01
select
	stat_date,
	'NEC' model_name,
	sec_org_desc,
	pu_name,
	phase,
	process,
	sum(work_hour) as work_hour
from (
	select
		from_unixtime(unix_timestamp(work_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd')  as stat_date,
		case when loan_t2 in ('PU1','PU2') then 'BOX'
			 when loan_t2 in ('PU3','PU4') then 'SMT'
		end as sec_org_desc,
		loan_t2 as pu_name,
		loan_t3 as t3_org_name,
		loan_t4 as t4_org_desc,
		case when loan_region = 'PCB' then 'PCB' when loan_region = 'Option' then 'SUB' else loan_region end as process,
		coalesce(model_name,'') as model_name,
		'MP' as phase,
		t1.totals as work_hour
	from lcfc.dwd_i_hr_iop_mh_d t1
	where t1.work_date >= '${first_day_of_month}'
	  and t1.work_date <= '${now_1}'
	  and t1.check_flag = '生效'
	  and t1.iop_type in ('IOP','REWORK')
	  and substr(t1.borrow_t4,1,3) = 'NEC'
	  and t1.loan_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
	  and t1.loan_t2 in ('PU4','PU3','PU1','PU2')
	) t1
group by stat_date,
	--model_name,
	sec_org_desc,
	pu_name,
	phase,
	process
	;
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_medion_hour_d_01
select
    stat_date,
    model_name1,
    model_name,
    sec_org_desc,
    pu_name,
    phase,
    coalesce(process,'-1') as process,
	work_hour,
    count(*) as num
from (
    select
        stat_date,
        model_name,
        model_name1,
        sec_org_desc,
        pu_name,
        phase,
        process,
		work_hour
    from(
        select
            stat_date,
            model_name,
            sec_org_desc,
            pu_name,
            phase,
            process,
            sum(work_hour) as work_hour
        from (
                select
					from_unixtime(unix_timestamp(work_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd')  as stat_date,
					case when loan_t2 in ('PU1','PU2') then 'BOX'
						 when loan_t2 in ('PU3','PU4') then 'SMT'
					end as sec_org_desc,
					loan_t2 as pu_name,
					loan_t3 as t3_org_name,
					loan_t4 as t4_org_desc,
					case when loan_region = 'PCB' then 'PCBA' when loan_region = 'Option' then 'SUB' else loan_region end as process,
					coalesce(model_name,'') as model_name,
					'MP' as phase,
                    t1.totals as work_hour
                from lcfc.dwd_i_hr_iop_mh_d t1
                where t1.work_date >= '${first_day_of_month}'
                  and t1.work_date <= '${now_1}'
                  and t1.check_flag = '生效'
                  and t1.iop_type in ('IOP','REWORK')
                  and substr(t1.borrow_t4,1,6) = 'Medion'
                  and t1.loan_t3 in ('BOX 生产管理部','BOX 功能测试部','BOX 物料管理部','SMT 主板生产部','SMT 主板测试加工部','SMT 物料管理部')
                  and t1.loan_t2 in ('PU4','PU3','PU1','PU2')
            ) t
        group by stat_date,
            model_name,
            sec_org_desc,
            pu_name,
            phase,
            process
        ) t
    lateral view explode(split(model_name, '/')) tmp as model_name1
    ) t
group by stat_date,
    model_name1,
    model_name,
    sec_org_desc,
    pu_name,
    phase,
    coalesce(process,'-1'),
    work_hour;
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_medion_hour_d_02
select
    t1.stat_date,
    t1.model_name1 as model_name,
    t1.sec_org_desc,
    t1.pu_name,
    t1.phase,
    t1.process,
    sum(coalesce(t1.work_hour,0)/t2.num) as work_hour
from lcfc.tmp_dwa_i_mfg_attend_medion_hour_d_01 t1
left join (
    select
        stat_date,
        model_name,
        sec_org_desc,
        pu_name,
        phase,
        coalesce(process,'-1') as process,
        count(model_name1) as num
    from lcfc.tmp_dwa_i_mfg_attend_medion_hour_d_01
    group by stat_date,
        model_name,
        sec_org_desc,
        pu_name,
        phase,
        coalesce(process,'-1')
    ) t2
  on t1.stat_date = t2.stat_date
 and t1.model_name = t2.model_name
 and t1.sec_org_desc =t2.sec_org_desc
 and t1.phase = t2.phase
 and t1.process = t2.process
 and t1.pu_name = t2.pu_name
group by t1.stat_date,
    t1.model_name1,
    t1.sec_org_desc,
    t1.pu_name,
    t1.phase,
    t1.process;

-- 11. 工时大包汇总
/**
  来源表：
  多个中间表（如 tmp_dwa_i_mfg_attend_*)
  代码逻辑：
   1. 将所有类型的工时合并，包括基础工时、IOP、NPI、DT、SSD、CB、NEC 等
   2. 通过左连接计算净工时（考虑借出和借入）
  输出表：
  lcfc.tmp_dwa_i_mfg_attend_all_hour_d_01
 */
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_all_hour_d_01
select
    t1.stat_date,
    t1.sec_org_desc,
    t1.group_process,
    t1.pu_name,
    t1.work_hour + coalesce(t2.work_hour,0) + coalesce(t4.work_hour,0) - coalesce(t6.work_hour,0) - coalesce(t7.work_hour,0) - coalesce(t8.work_hour,0) - coalesce(t9.work_hour,0) - coalesce(t10.work_hour,0) - coalesce(t8.work_hour,0) as work_hour
   -- t1.work_hour + coalesce(t2.work_hour,0) - coalesce(t4.work_hour,0) + coalesce(t5.work_hour,0) - coalesce(t6.work_hour,0) as work_hour
from (
    select
        t1.stat_date,
        t1.sec_org_desc,
        case when t1.group_process = 'PCB' then 'PCBA' else t1.group_process end as group_process,
        t1.pu_name,
        t1.work_hour
    from lcfc.tmp_dwa_i_mfg_attend_hcp_hour_d_03 t1     --DL HCP 工时
    ) t1
left join (
    select
        t1.stat_date,
        t1.sec_org_desc,
        case when t1.group_process = 'PCB' then 'PCBA' else t1.group_process end group_process,
        t1.pu_name,
        sum(coalesce(t1.borrow_hour - t1.loan_hour,0)) as work_hour
    from lcfc.tmp_dwa_i_mfg_attend_iop_hour_d_01 t1     --IOP,REWORK
    group by t1.stat_date,
        t1.sec_org_desc,
        case when t1.group_process = 'PCB' then 'PCBA' else t1.group_process end,
        t1.pu_name
    ) t2
  on t1.stat_date = t2.stat_date
 and t1.sec_org_desc = t2.sec_org_desc
 and t1.group_process = t2.group_process
 and t1.pu_name = t2.pu_name
left join (
    select
        t1.stat_date,
        t1.sec_org_desc,
        case when t1.process = 'PCB' then 'PCBA' else t1.process end as process,
        t1.pu_name,
        sum(work_hour) as work_hour
    from lcfc.tmp_dwa_i_mfg_attend_npi_hour_d_03 t1    --NPI not in (SVT,SOVP,MP)
    group by t1.stat_date,
        t1.sec_org_desc,
        case when t1.process = 'PCB' then 'PCBA' else t1.process end,
        t1.pu_name
    ) t4
  on t1.stat_date = t4.stat_date
 and t1.sec_org_desc = t4.sec_org_desc
 and t1.group_process = t4.process
 and t1.pu_name = t4.pu_name
--left join lcfc.tmp_dwa_i_mfg_attend_npi_hour_d_04 t5   -- NPI in (MP)
--  on t1.stat_date = t5.stat_date
-- and t1.sec_org_desc = t5.sec_org_desc
-- and t1.group_process = t5.group_process
-- and t1.pu_name = t5.pu_name
left join (
    select
        t1.stat_date,
        t1.sec_org_desc,
        case when t1.process = 'PCB' then 'PCBA' else t1.process end as process,
        t1.pu_name,
        sum(t1.work_hour) as work_hour
    from lcfc.tmp_dwa_i_mfg_attend_dt_hour_d_02 t1     --DT
    group by t1.stat_date,
        t1.sec_org_desc,
        case when t1.process = 'PCB' then 'PCBA' else t1.process end,
        t1.pu_name
    ) t6
  on t1.stat_date = t6.stat_date
 and t1.sec_org_desc = t6.sec_org_desc
 and t1.group_process = t6.process
 and t1.pu_name = t6.pu_name
left join (
    select
        t1.stat_date,
        'SMT' sec_org_desc,
        case when t1.process = 'PCB' then 'PCBA' else t1.process end as process,
        t1.pu_name,
        sum(t1.work_hour) as work_hour
    from lcfc.tmp_dwa_i_mfg_attend_smt_rs_hour_d_02 t1      --CCE
    group by t1.stat_date,
        t1.pu_name,
        case when t1.process = 'PCB' then 'PCBA' else t1.process end
    ) t7
  on t1.stat_date = t7.stat_date
 and t1.sec_org_desc = t7.sec_org_desc
 and t1.group_process = t7.process
 and t1.pu_name = t7.pu_name
left join (
    select
        t1.stat_date,
        t1.sec_org_desc,
        case when t1.process = 'PCB' then 'PCBA' else t1.process end as process,
        t1.pu_name,
        sum(t1.work_hour) as work_hour
    from lcfc.tmp_dwa_i_mfg_attend_ssd_hour_d_02 t1         --SSD
    group by t1.stat_date,
        t1.sec_org_desc,
        case when t1.process = 'PCB' then 'PCBA' else t1.process end,
        t1.pu_name
    ) t8
  on t1.stat_date = t8.stat_date
 and t1.sec_org_desc = t8.sec_org_desc
 and t1.group_process = t8.process
 and t1.pu_name = t8.pu_name
left join (
	select
		t1.stat_date,
        t1.sec_org_desc,
        case when t1.process = 'PCB' then 'PCBA' else t1.process end as process,
        t1.pu_name,
        sum(t1.work_hour) as work_hour
	from lcfc.tmp_dwa_i_mfg_attend_cb_hour_d_01 t1               --CB
	group by t1.stat_date,
        t1.sec_org_desc,
        case when t1.process = 'PCB' then 'PCBA' else t1.process end,
        t1.pu_name
	) t9
  on t1.stat_date = t9.stat_date
 and t1.sec_org_desc = t9.sec_org_desc
 and t1.group_process = t9.process
 and t1.pu_name = t9.pu_name
left join (
	select
		t1.stat_date,
        t1.sec_org_desc,
        case when t1.process = 'PCB' then 'PCBA' else t1.process end as process,
        t1.pu_name,
        sum(t1.work_hour) as work_hour
	from lcfc.tmp_dwa_i_mfg_attend_nec_hour_d_01 t1               --NEC
	group by t1.stat_date,
        t1.sec_org_desc,
        case when t1.process = 'PCB' then 'PCBA' else t1.process end,
        t1.pu_name
	) t10
  on t1.stat_date = t10.stat_date
 and t1.sec_org_desc = t10.sec_org_desc
 and t1.group_process = t10.process
 and t1.pu_name = t10.pu_name
left join (
    select
        t1.stat_date,
        t1.sec_org_desc,
        case when t1.process = 'PCB' then 'PCBA' else t1.process end as process,
        t1.pu_name,
        sum(t1.work_hour) as work_hour
    from lcfc.tmp_dwa_i_mfg_attend_medion_hour_d_02 t1         --Medion
    group by t1.stat_date,
        t1.sec_org_desc,
        case when t1.process = 'PCB' then 'PCBA' else t1.process end,
        t1.pu_name
    ) t11
  on t1.stat_date = t11.stat_date
 and t1.sec_org_desc = t11.sec_org_desc
 and t1.group_process = t11.process
 and t1.pu_name = t11.pu_name
;

-- 12. 生产线开线时长统计
/**
  来源表：
  lcfc.dw_i_mfg_box_rms_model_h、lcfc.dw_i_mfg_smt_rms_model_h
  代码逻辑：
   1. 统计生产线每天的开线时长
   2. 区分 SUB 段和其他段，并进行聚合
  输出表：
  lcfc.tmp_dwa_i_mfg_rms_hour_d_01 ~ lcfc.tmp_dwa_i_mfg_rms_hour_d_05
 */
--box开线时长
insert overwrite table lcfc.tmp_dwa_i_mfg_rms_hour_d_01
select
    t1.day_id as stat_date,
    t1.dns_name,
    case when t3.line_type = 'SUB' then concat(substr(t1.line_name,1,1),substr(substr(t1.line_name,1,5),-1,1))
         else concat(substr(t1.line_name,1,1),substr(t1.line_name,-1,1))
    end as line_name_short,
    t1.line_name,
    t3.line_type as process,
    t3.pu as pu_name,
    t1.model_name,
    t2.series_arr_box as series_arr,
    case when t1.phase in ('SVT','SOVP','MP') then 'MP'
         when t1.phase = 'N/A' then 'N/A'
         when t1.phase is null then 'N/A'
         else 'NPI'
    end as phase,
    t1.open_time_value
from lcfc.dw_i_mfg_box_rms_model_h t1
left join lcfc.dwd_n_img_arr_d t2
  on t1.model_name = t2.model_name
 and t2.day_id = '${now_1}'
left join lcfc.dim_n_line t3
  on t1.line_name = t3.line_name
where t1.day_id >= '${first_day_of_month}'
  and t1.day_id <= '${now_1}'
  and t1.line_name not in ('1PCBREPA','2PCBREPA')
  and coalesce(t2.product_type,'-999') not in ('SB','DT')
  and coalesce(t2.bu_business,'-999') not in ('SB','DT')
  and coalesce(t2.series_img,'-999') not in ('SSD','开天','海河','车载','漠河','梧桐山','滦河-DT','江淮','SE10-DT','亿道','奇瑞','魔点','红河','信创','Lark')
;

--SUB段时长1
insert overwrite table lcfc.tmp_dwa_i_mfg_rms_hour_d_02
select t.stat_date,
    t.dns_name,
    t.process,
    t.pu_name,
    t.line_name_short,
    t.model_name,
    t.series_arr,
    sum(case when open_time_value > 0 then 1 else 0 end) as open_line_times
from (
    select
        t1.stat_date,
        t1.dns_name,
        t1.process,
        t1.pu_name,
        t1.line_name_short,
        t1.line_name,
        t1.model_name,
        series_arr,
        sum(open_time_value) as open_time_value
    from lcfc.tmp_dwa_i_mfg_rms_hour_d_01 t1
    where t1.process = 'SUB'
    group by t1.stat_date,
        t1.dns_name,
        t1.process,
        t1.pu_name,
        t1.line_name_short,
        t1.line_name,
        t1.model_name,
        series_arr
    ) t
group by t.stat_date,
    t.dns_name,
    t.process,
    t.pu_name,
    t.line_name_short,
    t.model_name,
    series_arr
;

--SUB段时长2
insert overwrite table lcfc.tmp_dwa_i_mfg_rms_hour_d_03
select
    t1.stat_date,
    t1.process,
    t1.pu_name,
    t1.line_name_short,
    t1.line_name,
    t1.model_name,
    t1.series_arr,
    t1.phase,
    sum(open_time_value) as open_time_value
from (
    select t1.stat_date,
        t1.dns_name,
        t1.process,
        t1.pu_name,
        t1.line_name_short,
        t1.line_name,
        t1.model_name,
        t1.series_arr,
        t1.phase,
        sum(t1.open_time_value)/t2.open_line_times as open_time_value
    from lcfc.tmp_dwa_i_mfg_rms_hour_d_01 t1
    left join lcfc.tmp_dwa_i_mfg_rms_hour_d_02 t2
      on t1.stat_date = t2.stat_date
     and t1.dns_name = t2.dns_name
     and t1.process = t2.process
     and t1.pu_name = t2.pu_name
     and t1.line_name_short = t2.line_name_short
     and t1.model_name = t2.model_name
    where t1.process = 'SUB'
    group by t1.stat_date,
        t1.dns_name,
        t1.process,
        t1.pu_name,
        t1.line_name_short,
        t1.line_name,
        t1.model_name,
        t1.series_arr,
        t1.phase,
        t2.open_line_times
    ) t1
group by t1.stat_date,
    t1.process,
    t1.pu_name,
    t1.line_name_short,
    t1.line_name,
    t1.model_name,
    t1.series_arr,
    t1.phase

-- BOX开线时长结果
insert overwrite table lcfc.tmp_dwa_i_mfg_rms_hour_d_04
select
    t1.stat_date,
    t1.process,
    t1.pu_name,
    t1.line_name,
    t1.model_name,
    series_arr,
    t1.phase,
    sum(open_time_value) as open_time_value
from lcfc.tmp_dwa_i_mfg_rms_hour_d_01 t1
where t1.process <> 'SUB'
group by t1.stat_date,
    t1.process,
    t1.pu_name,
    t1.line_name,
    t1.model_name,
    series_arr,
    t1.phase
union all
select
    t1.stat_date,
    t1.process,
    t1.pu_name,
    t1.line_name,
    t1.model_name,
    series_arr,
    t1.phase,
    t1.open_time_value
from lcfc.tmp_dwa_i_mfg_rms_hour_d_03 t1
;

-- SMT开线时长
insert overwrite table lcfc.tmp_dwa_i_mfg_rms_hour_d_05
select
    t1.day_id as stat_date,
    '' dns_name,
    concat(substr(t1.line_name,1,1),substr(t1.line_name,-1,1)) as line_name_short,
    t1.line_name,
    case when substr(t3.line_type,1,3) = 'PCB' then 'PCBA' else substr(t3.line_type,1,3) end as process,
    t3.pu as pu_name,
    substr(t1.model_name,1,5) as model_name,
    t2.series_arr_smt as series_arr,
    case when t1.phase in ('SVT','SOVP','MP') then 'MP'
         when t1.phase = 'N/A' then 'N/A'
         when t1.phase is null then 'N/A'
         else 'NPI'
    end as phase,
    sum(t1.open_time_value) as open_time_value
from lcfc.dw_i_mfg_smt_rms_model_h t1
left join lcfc.dwd_n_img_arr_d t2
  on substr(t1.model_name,1,5) = t2.model_name
 and t2.day_id = '${now_1}'
left join lcfc.dim_n_line t3
  on t1.line_name = t3.line_name
where t1.day_id >= '${first_day_of_month}'
  and t1.day_id <= '${now_1}'
  and t1.line_name not in ('1PCBREPA','2PCBREPA')
  and coalesce(t2.product_type,'-999') not in ('SB','DT')
  and coalesce(t2.bu_business,'-999') not in ('SB','DT')
  and coalesce(t2.series_img,'-999') not in ('SSD','开天','海河','车载','漠河','梧桐山','滦河-DT','江淮','SE10-DT','亿道','奇瑞','魔点','红河','信创','Lark')
group by t1.day_id,
    concat(substr(t1.line_name,1,1),substr(t1.line_name,-1,1)),
    t1.line_name,
    case when substr(t3.line_type,1,3) = 'PCB' then 'PCBA' else substr(t3.line_type,1,3) end,
    t3.pu,
    substr(t1.model_name,1,5),
    t2.series_arr_smt,
    case when t1.phase in ('SVT','SOVP','MP') then 'MP'
         when t1.phase = 'N/A' then 'N/A'
         when t1.phase is null then 'N/A'
         else 'NPI'
    end

-- 13. 小线/大线编制人数
/**
  来源表：
  lcfc.dwd_a_mfg_line_type_h、lcfc.dwd_a_hr_organization_d
  代码逻辑：
   1. 统计小线和大线的编制人数
   2. 根据产线类型和型号进行匹配
  输出表：
  lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_01 ~ lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_06
 */
-- 小线编制
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_01
select
    t1.stat_date,
    --t1.dns_name,
    t1.pu_name,
    t1.line_name,
    t1.process,
    sum(t3.organization_num) as organization_num
from (
    select
        t1.shift_date as stat_date,
        case when t1.shift = '白班' then 1 else 2 end as dns_name,
        t1.plant_code as pu_name,
        t1.line_name,
        t2.line_type as process,
        case when t2.line_type = 'SUB' then '小线编制Ⅱ-有SUB' else '小线编制Ⅰ-无SUB' end as organization_name
    from lcfc.dwd_a_mfg_line_type_h t1
    left join lcfc.dim_n_line t2
      on t1.line_name = t2.line_name
    where t1.work_type = '小线'
      and t1.shift_date = '${now_1}'
    ) t1
left join lcfc.dwd_a_hr_organization_d t2
  on t1.pu_name = t2.pu_name
 and t1.organization_name = t2.organization_name
 and t2.use_flag = 'Y'
left join lcfc.dwd_a_hr_organization_detail_d t3
  on t2.organization_id = t3.organization_id
 and t3.day_id = '${now_1}'
group by t1.stat_date,
    --t1.dns_name,
    t1.pu_name,
    concat(substr(t1.line_name,1,1),substr(t1.line_name,-1,1)),
    t1.process,
    t1.line_name

-- 小线编制2
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_02
select
    t2.stat_date,
    t2.process,
    t2.pu_name,
    t2.line_name,
    t1.model_name,
    t1.phase,
    t1.open_time_value * nvl(t2.organization_num,0) as open_time_organization
from lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_01 t2
left join lcfc.tmp_dwa_i_mfg_rms_hour_d_04 t1
  on t1.stat_date = t2.stat_date
 --and t1.dns_name = t2.dns_name
 and t1.line_name = t2.line_name
 and t1.pu_name = t2.pu_name
 and t1.process = t2.process

-- 大线编制
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_031
select
	t1.organization_id,
	plant_name,
	t1.organization_name,
	case when trim(t1.line_name) = '' then '-999' else coalesce(t1.line_name,'-999') end as line_name,
	img_serial,
	current_version
from lcfc.ods_a_r_dl1_t_20100_d t1;
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_03
select
	organization_series,
	organization_id,
	pu_name,
	organization_name,
	line_name,
	case when coalesce(substr(line_name,2,length(line_name)-2),'-999') = 'PCB' then 'PCBA'
         when line_name = '-999' then '-999'
        else coalesce(substr(line_name,2,length(line_name)-2),'-999')
    end as process
from (
	select
		coalesce(t2.img_series,'-999') as organization_series,
		t1.organization_id,
		plant_name as pu_name,
		t1.organization_name,
		case when trim(t3.line_name) = '' then '-999' else coalesce(t3.line_name,'-999') end as line_name,
		current_version,
		row_number() over(partition by coalesce(t2.img_series,'-999'),t1.organization_id,plant_name,t1.organization_name,coalesce(t3.line_name,'-999') order by current_version desc) as row_num
	from lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_031 t1
	lateral view explode(split(img_serial,',')) t2 as img_series
	lateral view explode(split(coalesce(line_name,''),',')) t3 as line_name
	) t
where t.row_num = 1
;

-- 大线编制(spark)
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_04
select
	t1.organization_series,
    t1.organization_id,
    t1.organization_name,
    t1.line_name,
    t1.pu_name,
    t1.process,
    t2.group_process,
    case when t2.group_process in ('SUB','PACK','ASSY') then 'BOX'
         when t2.group_process in ('SMT','PCBA') then 'SMT'
         else '-999'
    end as process_flag,
    --t2.organization_id,
    t2.version_flag,
    t2.organization_num,
    row_number() over(partition by t1.organization_series,t1.organization_id,t1.organization_name,t1.line_name,t1.pu_name,t1.process,t2.group_process order by version_flag desc) as row_num
from lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_03 t1
left join (
	select
		organization_id,
		case when group_name in ('SMTC','SMTS') then 'SMT' else group_name end as group_process,
		version as version_flag,
		sum(organization_qty) as organization_num
	from lcfc.ods_a_r_dl1_detail_t_20101_d
	group by organization_id,
		case when group_name in ('SMTC','SMTS') then 'SMT' else group_name end,
		version
	) t2
  on t1.organization_id = t2.organization_id
 and t1.process = t2.group_process
where t1.process in ('SMT','SUB','PACK','ASSY','PCBA')
  and t1.pu_name in ('PU1','PU2','PU3','PU4')
union all
select
    t1.organization_series,
    t1.organization_id,
    t1.organization_name,
    t1.line_name,
    t1.pu_name,
    t1.process,
    t2.group_process,
    case when t2.group_process in ('SUB','PACK','ASSY') then 'BOX'
         when t2.group_process in ('SMT','PCBA') then 'SMT'
         else '-999'
    end as process_flag,
    --t2.organization_id,
    t2.version_flag,
    t2.organization_num,
    row_number() over(partition by t1.organization_series,t1.organization_id,t1.organization_name,t1.line_name,t1.pu_name,t1.process,t2.group_process order by version_flag desc) as row_num
from lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_03 t1
left join (
    select
        organization_id,
        case when group_name in ('SMTC','SMTS') then 'SMT' else group_name end as group_process,
        version as version_flag,
        sum(organization_qty) organization_num
    from lcfc.ods_a_r_dl1_detail_t_20101_d
    group by organization_id,
        case when group_name in ('SMTC','SMTS') then 'SMT' else group_name end,
        version
    ) t2
    on t1.organization_id = t2.organization_id
where t1.process not in ('SMT','SUB','PACK','ASSY','PCBA')
  and t1.pu_name in ('PU1','PU2','PU3','PU4')
;

-- 大线编制结果
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_05
select
    t1.stat_date,
    t1.process,
    t1.pu_name,
    case when t1.pu_name in ('PU1','PU2') then 'BOX'
         when t1.pu_name in ('PU3','PU4') then 'SMT'
    end as sec_org_desc,
    t1.line_name,
	t1.series_arr,
    t1.model_name,
    t1.phase,
    --t1.open_time_value,
    --t1.organization_num,
    t1.open_time_value * organization_num as open_time_organization
from (
    select
        t1.stat_date,
        t1.process,
        t1.pu_name,
        t1.line_name,
		t1.series_arr,
        t1.model_name,
        t1.phase,
        t1.open_time_value,
        case when t2.organization_num is not null then t2.organization_num
             when t2.organization_num is null and t3.organization_num is not null then t3.organization_num
             when t2.organization_num is null and t3.organization_num is null and t4.organization_num is not null then t4.organization_num
            -- when t2.organization_num is null and t3.organization_num is null and t4.organization_num is null and t5.organization_num is not null then t5.organization_num
             else 0
        end as organization_num
    from lcfc.tmp_dwa_i_mfg_rms_hour_d_04 t1
    left join lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_04 t2
      on t1.series_arr = t2.organization_series
     and t1.line_name = t2.line_name
     and t2.row_num = 1
    left join lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_04 t3
      on t1.line_name = t3.line_name
     and t3.organization_series = '-999'
     and t3.row_num = 1
    left join lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_04 t4
      on t1.series_arr = t4.organization_series
     and t1.pu_name = t4.pu_name
     and t1.process = t4.group_process
     and t4.line_name = '-999'
     and t4.row_num = 1
    --left join lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_04 t5
    --  on t1.pu_name = t5.pu_name
    -- and t1.process = t5.group_process
    -- and t5.organization_series = '-999'
    -- and t5.line_name = '-999'
    -- and t5.row_num = 1
    left join (
        select distinct
            line_name,shift_date,work_type
        from lcfc.dwd_a_mfg_line_type_h
        ) t6
      on t1.line_name = t6.line_name
     and t1.stat_date = t6.shift_date
    where coalesce(t6.work_type,'') <> '小线'
      and t1.phase <> 'NPI'
    ) t1
;

-- 大线编制1
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_06
select
    t1.stat_date,
    t1.process,
    t1.pu_name,
    case when t1.pu_name in ('PU1','PU2') then 'BOX'
         when t1.pu_name in ('PU3','PU4') then 'SMT'
    end as sec_org_desc,
    t1.line_name,
    t1.series_arr,
    t1.model_name,
    t1.phase,
    t1.open_time_value * organization_num as open_time_organization
from (
    select
        t1.stat_date,
        t1.process,
        t1.pu_name,
        t1.line_name,
        t1.series_arr,
        t1.model_name,
        t1.phase,
        t1.open_time_value,
        case when t2.organization_num is not null then t2.organization_num
             when t2.organization_num is null and t3.organization_num is not null then t3.organization_num
             when t2.organization_num is null and t3.organization_num is null and t4.organization_num is not null then t4.organization_num
            -- when t2.organization_num is null and t3.organization_num is null and t4.organization_num is null and t5.organization_num is not null then t5.organization_num
             else 0
        end as organization_num
    from lcfc.tmp_dwa_i_mfg_rms_hour_d_05 t1
    left join lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_04 t2
      on t1.series_arr = t2.organization_series
     and t1.line_name = t2.line_name
     and t2.row_num = 1
    left join lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_04 t3
      on t1.line_name = t3.line_name
     and t3.organization_series = '-999'
     and t3.row_num = 1
    left join lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_04 t4
      on t1.series_arr = t4.organization_series
     and t1.pu_name = t4.pu_name
     and t1.process = t4.group_process
     and t4.line_name = '-999'
     and t4.row_num = 1
    --left join lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_04 t5
    --  on t1.pu_name = t5.pu_name
    -- and t1.process = t5.group_process
    -- and t5.organization_series = '-999'
    -- and t5.line_name = '-999'
    -- and t5.row_num = 1
    left join (
        select distinct
            line_name,shift_date,work_type
        from lcfc.dwd_a_mfg_line_type_h
        ) t6
      on t1.line_name = t6.line_name
     and t1.stat_date = t6.shift_date
    where coalesce(t6.work_type,'') <> '小线'
      and t1.phase <> 'NPI'
    ) t1
;

-- 14. 工时比例计算
/**
  来源表：
  lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_05、lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_06
  代码逻辑：
   计算各产品在不同部门的工时占比
  输出表：
  lcfc.tmp_dwa_i_mfg_attend_hour_arr_cb_d_07
  lcfc.tmp_dwa_i_mfg_attend_hour_arr_nec_d_07
  lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_07
 */
insert overwrite table lcfc.tmp_dwa_i_mfg_attend_hour_arr_cb_d_07
select
    '${now_1}' stat_date,
    t1.process,
    t1.sec_org_desc,
    t1.pu_name,
    t1.model_name,
    t1.phase,
    t1.open_time_organization,
    nvl(t2.open_time_organization,0) as open_time_organization_total,
    nvl(t1.open_time_organization / t2.open_time_organization ,0) as open_time_rate
from (
    select
        t1.process,
        t1.sec_org_desc,
        t1.pu_name,
        t1.model_name,
        t1.phase,
        sum(open_time_organization) as open_time_organization
    from lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_05 t1
	where series_arr like '%CB%'
    group by t1.process,
        t1.sec_org_desc,
        t1.pu_name,
        t1.model_name,
        t1.phase
    ) t1
left join (
    select
        t1.sec_org_desc,
        sum(t1.open_time_organization) as open_time_organization
    from lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_05 t1
	where series_arr like '%CB%'
    group by t1.sec_org_desc
    ) t2
  on t1.sec_org_desc = t2.sec_org_desc

union all

select
    '${now_1}' stat_date,
    t1.process,
    t1.sec_org_desc,
    t1.pu_name,
    t1.model_name,
    t1.phase,
    t1.open_time_organization,
    nvl(t2.open_time_organization,0) as open_time_organization_total,
    nvl(t1.open_time_organization / t2.open_time_organization ,0) as open_time_rate
from (
    select
        t1.process,
        t1.sec_org_desc,
        t1.pu_name,
        t1.model_name,
        t1.phase,
        sum(open_time_organization) as open_time_organization
    from lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_06 t1
	where series_arr like '%CB%'
    group by t1.process,
        t1.sec_org_desc,
        t1.pu_name,
        t1.model_name,
        t1.phase
    ) t1
left join (
    select
        t1.sec_org_desc,
        sum(t1.open_time_organization) as open_time_organization
    from lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_06 t1
	where series_arr like '%CB%'
    group by t1.sec_org_desc
    ) t2
  on t1.sec_org_desc = t2.sec_org_desc
;


insert overwrite table lcfc.tmp_dwa_i_mfg_attend_hour_arr_nec_d_07
select
    '${now_1}' stat_date,
    t1.process,
    t1.sec_org_desc,
    t1.pu_name,
    t1.model_name,
    t1.phase,
    t1.open_time_organization,
    nvl(t2.open_time_organization,0) as open_time_organization_total,
    nvl(t1.open_time_organization / t2.open_time_organization ,0) as open_time_rate
from (
    select
        --t1.stat_date,
        t1.process,
        t1.sec_org_desc,
        t1.pu_name,
        t1.model_name,
        t1.phase,
        sum(open_time_organization) as open_time_organization
    from lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_05 t1
	where series_arr like '%NEC%'
    group by --t1.stat_date,
        t1.process,
        t1.sec_org_desc,
        t1.pu_name,
        t1.model_name,
        t1.phase
    ) t1
left join (
    select
        t1.sec_org_desc,
        sum(t1.open_time_organization) as open_time_organization
    from lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_05 t1
	where series_arr like '%NEC%'
    group by t1.sec_org_desc
    ) t2
  on t1.sec_org_desc = t2.sec_org_desc

union all

select
    '${now_1}' stat_date,
    t1.process,
    t1.sec_org_desc,
    t1.pu_name,
    t1.model_name,
    t1.phase,
    t1.open_time_organization,
    nvl(t2.open_time_organization,0) as open_time_organization_total,
    nvl(t1.open_time_organization / t2.open_time_organization ,0) as open_time_rate
from (
    select
        t1.process,
        t1.sec_org_desc,
        t1.pu_name,
        t1.model_name,
        t1.phase,
        sum(open_time_organization) as open_time_organization
    from lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_06 t1
	where series_arr like '%NEC%'
    group by t1.process,
        t1.sec_org_desc,
        t1.pu_name,
        t1.model_name,
        t1.phase
    ) t1
left join (
    select
        t1.sec_org_desc,
        sum(t1.open_time_organization) as open_time_organization
    from lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_06 t1
	where series_arr like '%NEC%'
    group by t1.sec_org_desc
    ) t2
  on t1.sec_org_desc = t2.sec_org_desc
;

insert overwrite table lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_07
select
    t1.stat_date,
    t1.process,
    t1.sec_org_desc,
    t1.pu_name,
    t1.model_name,
    t1.phase,
    t1.open_time_organization,
    nvl(t2.open_time_organization,0) as open_time_organization_total,
    nvl(t1.open_time_organization / t2.open_time_organization ,0) as open_time_rate
from (
    select
        t1.stat_date,
        t1.process,
        t1.sec_org_desc,
        t1.pu_name,
        t1.model_name,
        t1.phase,
        sum(open_time_organization) as open_time_organization
    from lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_05 t1
	where series_arr not like '%CB%'
	  and series_arr not like '%NEC%'
    group by t1.stat_date,
        t1.process,
        t1.sec_org_desc,
        t1.pu_name,
        t1.model_name,
        t1.phase
    ) t1
left join (
    select
        t1.stat_date,
        t1.sec_org_desc,
        t1.pu_name,
        t1.process,
        sum(t1.open_time_organization) as open_time_organization
    from lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_05 t1
	where series_arr not like '%CB%'
	  and series_arr not like '%NEC%'
    group by t1.stat_date,
        t1.sec_org_desc,
        t1.pu_name,
        t1.process
    ) t2
  on t1.stat_date = t2.stat_date
 and t1.sec_org_desc = t2.sec_org_desc
 and t1.pu_name = t2.pu_name
 and t1.process = t2.process
union all
select
    t1.stat_date,
    t1.process,
    t1.sec_org_desc,
    t1.pu_name,
    t1.model_name,
    t1.phase,
    t1.open_time_organization,
    nvl(t2.open_time_organization,0) as open_time_organization_total,
    nvl(t1.open_time_organization / t2.open_time_organization ,0) as open_time_rate
from (
    select
        t1.stat_date,
        t1.process,
        t1.sec_org_desc,
        t1.pu_name,
        t1.model_name,
        t1.phase,
        sum(open_time_organization) as open_time_organization
    from lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_06 t1
	where series_arr not like '%CB%'
	  and series_arr not like '%NEC%'
    group by t1.stat_date,
        t1.process,
        t1.sec_org_desc,
        t1.pu_name,
        t1.model_name,
        t1.phase
    ) t1
left join (
    select
        t1.stat_date,
        t1.sec_org_desc,
        t1.pu_name,
        t1.process,
        sum(t1.open_time_organization) as open_time_organization
    from lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_06 t1
	where series_arr not like '%CB%'
	  and series_arr not like '%NEC%'
    group by t1.stat_date,
        t1.sec_org_desc,
        t1.pu_name,
        t1.process
    ) t2
  on t1.stat_date = t2.stat_date
 and t1.sec_org_desc = t2.sec_org_desc
 and t1.pu_name = t2.pu_name
 and t1.process = t2.process

-- 15. 最终结果表插入
/**
  来源表：
  所有中间表
  代码逻辑：
   1. 汇总所有类型的工时，并乘以相应的工时比例
   2. 按财务年份、月份、产品型号、部门、阶段、流程等字段进行分组统计
  输出表：
  lcfc.dwa_i_mfg_attend_hour_d（带分区）
 */
insert overwrite table lcfc.dwa_i_mfg_attend_hour_d partition(day_id = '${now_1}')
select
    t2.fin_year,
    t2.month_of_fin_year,
    t1.model_name,
    t1.sec_org_desc as department,
    t1.phase,
    t1.process,
    case when sum(att_hours) < 0 then 0 else sum(att_hours) end as att_hours
from (
    select
        t1.stat_date,
        t1.sec_org_desc,
        t1.group_process as process,
        t2.phase,
        t2.model_name,
        round(t1.work_hour * coalesce(t2.open_time_rate,0),6) as att_hours
    from lcfc.tmp_dwa_i_mfg_attend_all_hour_d_01 t1
    left join lcfc.tmp_dwa_i_mfg_attend_hour_arr_d_07 t2
      on t1.stat_date = t2.stat_date
     and t1.sec_org_desc = t2.sec_org_desc
     and t1.pu_name = t2.pu_name
     and t1.group_process = t2.process
    where t2.phase <> 'N/A'
    union all
    --CB
    select
        t2.stat_date,
        t1.sec_org_desc,
        t2.process as process,
        t2.phase,
        t2.model_name,
        round(t1.work_hour * coalesce(t2.open_time_rate,0),6) as att_hours
    from (
        select
            t1.sec_org_desc,
            sum(work_hour) as work_hour
        from lcfc.tmp_dwa_i_mfg_attend_cb_hour_d_01 t1
        group by t1.sec_org_desc
        ) t1
    left join lcfc.tmp_dwa_i_mfg_attend_hour_arr_cb_d_07 t2
      on t1.sec_org_desc = t2.sec_org_desc
     --and t1.stat_date = t2.stat_date
     --and t1.pu_name = t2.pu_name
     --and t1.process = t2.process
    where t2.phase <> 'N/A'
    union all
    --NEC
    select
        t2.stat_date,
        t1.sec_org_desc,
        t2.process as process,
        t2.phase,
        t2.model_name,
        round(t1.work_hour * coalesce(t2.open_time_rate,0),6) as att_hours
    from (
        select
            t1.sec_org_desc,
            sum(work_hour) as work_hour
        from lcfc.tmp_dwa_i_mfg_attend_nec_hour_d_01 t1
        group by t1.sec_org_desc
        ) t1
    left join lcfc.tmp_dwa_i_mfg_attend_hour_arr_nec_d_07 t2
      on t1.sec_org_desc = t2.sec_org_desc
     --and t1.stat_date = t2.stat_date
     --and t1.pu_name = t2.pu_name
     --and t1.process = t2.process
    where t2.phase <> 'N/A'
    union all
    --MTY
    select
        stat_date,
        sec_org_desc,
        process,
        phase,
        model_name,
        att_hours
    from lcfc.tmp_dwa_i_mfg_attend_mty_hour_d_03 t1
    where t1.phase <> 'N/A'
    union all
    --DT
    select
        stat_date,
        sec_org_desc,
        process,
        phase,
        model_name,
        work_hour as att_hours
    from lcfc.tmp_dwa_i_mfg_attend_dt_hour_d_02 t1
    where t1.phase <> 'N/A'
    union all
    --Rework
    select
        stat_date,
        sec_org_desc,
        process,
        phase,
        model_name,
        abs(work_hour) as att_hours
    from lcfc.tmp_dwa_i_mfg_attend_rework_hour_d_02 t1
    where t1.phase <> 'N/A'
    union all
    --NPI
    select
        stat_date,
        sec_org_desc,
        process,
        phase,
        model_name,
        abs(work_hour) as att_hours
    from lcfc.tmp_dwa_i_mfg_attend_npi_hour_d_03 t1
    where t1.phase <> 'N/A'
    union all
    --SSD
    select
        stat_date,
        sec_org_desc,
        process as process,
        phase,
        model_name,
        abs(work_hour) as att_hours
    from lcfc.tmp_dwa_i_mfg_attend_ssd_hour_d_02 t1
    where t1.phase <> 'N/A'
	union all
    --lark
    select
        stat_date,
        sec_org_desc,
        process as process,
        phase,
        model_name,
        abs(work_hour) as att_hours
    from lcfc.tmp_dwa_i_mfg_attend_lark_hour_d_02 t1
    where t1.phase <> 'N/A'
    union all
    --medion
    select
        stat_date,
        sec_org_desc,
        process as process,
        phase,
        model_name,
        abs(work_hour) as att_hours
    from lcfc.tmp_dwa_i_mfg_attend_medion_hour_d_02 t1
    where t1.phase <> 'N/A'
    ) t1
left join lcfc.dim_n_date t2 on t1.stat_date = t2.`date`
where length(model_name) <= 5
  and process in ('PCBA','PACK','ASSY','SUB','SMT')
group by t2.fin_year,
    t2.month_of_fin_year,
    t1.model_name,
    t1.sec_org_desc,
    t1.phase,
    t1.process

