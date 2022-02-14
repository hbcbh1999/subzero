// list of passing tests
// x EmbedInnerJoinSpec.hs
// x HtmlRawOutputSpec.hs
// x RangeSpec.hs
// x RawOutputTypesSpec.hs
// x ErrorSpec.hs

// UpsertSpec.hs 
// SingularSpec.hs
// UnicodeSpec.hs
// UpdateSpec.hs
// AndOrParamsSpec.hs
// DeleteSpec.hs
// EmbedDisambiguationSpec.hs
// InsertSpec.hs
// JsonOperatorSpec.hs
// MultipleSchemaSpec.hs
// QueryLimitedSpec.hs
// QuerySpec.hs
// RpcSpec.hs


mod common;

mod query;
mod rpc;
mod auth;
mod insert;
mod embed_disambiguation;
mod and_or_param;
mod json_operator;
mod limits;
mod multiple_schemas;
mod delete;
mod update;
mod unicode;
mod singular;
mod upsert;