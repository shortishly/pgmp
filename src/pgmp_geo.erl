%% Copyright (c) 2022 Peter Morgan <peter.james.morgan@gmail.com>
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.


-module(pgmp_geo).


-export_type([box/0]).
-export_type([circle/0]).
-export_type([line/0]).
-export_type([lseg/0]).
-export_type([path/0]).
-export_type([point/0]).
-export_type([polygon/0]).


-type point() :: #{x := float(), y := float()}.

-type path() :: #{path := closed | open, points := [point()]}.

-type polygon() :: [point()].

-type circle() :: #{x := float(), y := float(), radius := float()}.

-type line() :: #{a := float(), b := float(), c := float()}.

-type lseg() :: {point(), point()}.

-type box() :: {point(), point()}.
