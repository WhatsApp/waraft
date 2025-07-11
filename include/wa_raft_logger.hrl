-include_lib("kernel/include/logger.hrl").

-define(RAFT_LOG_OPTS, #{domain => [whatsapp, wa_raft]}).

-define(RAFT_LOG_ERROR(Message), ?LOG_ERROR(Message, ?RAFT_LOG_OPTS)).
-define(RAFT_LOG_ERROR(Format, Args), ?LOG_ERROR(Format, Args, ?RAFT_LOG_OPTS)).

-define(RAFT_LOG_WARNING(Message), ?LOG_WARNING(Message, ?RAFT_LOG_OPTS)).
-define(RAFT_LOG_WARNING(Format, Args), ?LOG_WARNING(Format, Args, ?RAFT_LOG_OPTS)).

-define(RAFT_LOG_NOTICE(Message), ?LOG_NOTICE(Message, ?RAFT_LOG_OPTS)).
-define(RAFT_LOG_NOTICE(Format, Args), ?LOG_NOTICE(Format, Args, ?RAFT_LOG_OPTS)).

-define(RAFT_LOG_INFO(Message), ?LOG_INFO(Message, ?RAFT_LOG_OPTS)).
-define(RAFT_LOG_INFO(Format, Args), ?LOG_INFO(Format, Args, ?RAFT_LOG_OPTS)).

-define(RAFT_LOG_DEBUG(Message), ?LOG_DEBUG(Message, ?RAFT_LOG_OPTS)).
-define(RAFT_LOG_DEBUG(Format, Args), ?LOG_DEBUG(Format, Args, ?RAFT_LOG_OPTS)).

-define(RAFT_LOG(Level, Message), ?LOG(Level, Message, ?RAFT_LOG_OPTS)).
-define(RAFT_LOG(Level, Format, Args), ?LOG(Level, Format, Args, ?RAFT_LOG_OPTS)).
