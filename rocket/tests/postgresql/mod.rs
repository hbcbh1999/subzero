// Copyright (c) 2022-2025 subZero Cloud S.R.L
//
// This file is part of subZero - The All-in-One library suite for internal tools development
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
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

mod setup;

#[allow(unused_imports)]
mod aggregates;
#[allow(unused_imports)]
mod and_or_param;
#[allow(unused_imports)]
mod auth;
#[allow(unused_imports)]
mod custom_relations;
#[allow(unused_imports)]
mod delete;
#[allow(unused_imports)]
mod embed_disambiguation;
#[allow(unused_imports)]
mod insert;
#[allow(unused_imports)]
mod json_operator;
#[allow(unused_imports)]
mod limits;
#[allow(unused_imports)]
mod multiple_schemas;
// #[allow(unused_imports)]
// mod permissions;
#[allow(unused_imports)]
mod query;
#[allow(unused_imports)]
mod rpc;
#[allow(unused_imports)]
mod singular;
#[allow(unused_imports)]
mod unicode;
#[allow(unused_imports)]
mod update;
#[allow(unused_imports)]
mod upsert;

#[allow(clippy::duplicate_mod)]
#[allow(unused_imports)]
#[path = "../common/permissions.rs"]
mod permissions;

#[allow(clippy::duplicate_mod)]
#[allow(unused_imports)]
#[path = "../common/basic.rs"]
mod basic;
