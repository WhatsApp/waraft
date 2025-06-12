%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This file defines dialyzer types.

-module(wa_raft).
-compile(warn_missing_spec_all).

-include_lib("wa_raft/include/wa_raft.hrl").

%% Public Types
-export_type([
    table/0,
    partition/0,
    error/0,
    args/0,
    identity/0
]).

-type table() :: atom().
-type partition() :: pos_integer().
-type error() :: {error, term()}.

%% Specification for starting a RAFT partition.
-type args() ::
    #{
        % Table name
        table := table(),
        % Partition number
        partition := partition(),
        % Distribution module
        distribution_module => module(),
        % Log module
        log_module => module(),
        % Log label module
        label_module => module(),
        % Storage module
        storage_module => module(),
        % Transport module
        transport_module => module()
    }.

-type identity() :: #raft_identity{}.
