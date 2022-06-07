//use core::fmt;

use super::base::{
    cast_select_item_format, fmt_as, fmt_body, fmt_condition, fmt_condition_tree, fmt_count_query, fmt_field, fmt_field_format, fmt_filter,
    fmt_identity, fmt_in_filter, fmt_json_operand, fmt_json_operation, fmt_json_path, fmt_limit, fmt_logic_operator, fmt_main_query, fmt_offset,
    fmt_operator, fmt_order, fmt_order_term, fmt_groupby, fmt_groupby_term, fmt_qi, fmt_query, fmt_select_item, fmt_select_name, fmt_sub_select_item, return_representation,
    simple_select_item_format, star_select_item_format, fmt_function_param, fmt_select_item_function,
};
use crate::api::{Condition::*, ContentType::*, Filter::*, Join::*, JsonOperand::*, JsonOperation::*, LogicOperator::*, QueryNode::*, SelectItem::*, *};
use crate::dynamic_statement::{param, sql, JoinIterator, SqlSnippet};
use crate::error::Result;
//use clickhouse::{Client,sql::Bind};
//use bytes::{BufMut, BytesMut};
//use postgres_types::{to_sql_checked, Format, IsNull, ToSql, Type};
// use postgres_types::{ToSql};
//use std::error::Error;

pub enum Param<'a> {
    LV(&'a ListVal),
    SV(&'a SingleVal),
    PL(&'a Payload),
}
// helper type aliases
pub trait ToSql {
    fn to_param(&self) -> Param;
}
pub type SqlParam<'a> = (dyn ToSql + Sync + 'a);
pub type Snippet<'a> = SqlSnippet<'a, SqlParam<'a>>;


impl ToSql for ListVal {
    fn to_param(&self) -> Param {
        Param::LV(self)
    }
}

impl ToSql for SingleVal {
    fn to_param(&self) -> Param {
        Param::SV(self)
    }
}

impl ToSql for Payload {
    fn to_param(&self) -> Param {
        Param::PL(self)
    }
}




fmt_main_query!();
fmt_query!();
fmt_count_query!();
fmt_body!();
fmt_condition_tree!();
fmt_condition!();
fmt_filter!();
fmt_select_name!();
fmt_select_item!();
fmt_select_item_function!();
fmt_function_param!();
fmt_sub_select_item!();
fmt_operator!();
fmt_logic_operator!();
fmt_identity!();
fmt_qi!();
fmt_field!();
fmt_order!();
fmt_order_term!();
fmt_groupby!();
fmt_groupby_term!();
fmt_as!();
fmt_limit!();
fmt_offset!();
fmt_json_path!();
fmt_json_operation!();
fmt_json_operand!();
