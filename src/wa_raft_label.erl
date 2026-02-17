%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% Pluggable module for labeling log entries before adding them to the RAFT log.

-module(wa_raft_label).
-compile(warn_missing_spec_all).

    -type label() :: dynamic().

-export_type([label/0]).

%%% ------------------------------------------------------------------------
%%%  Behaviour callbacks
%%%

% Produce a label for a new log record based on the log payload and the label of the preceding log entry.
-callback new_label(LastLabel :: label(), Command :: wa_raft_acceptor:command()) -> NewLabel :: label().
