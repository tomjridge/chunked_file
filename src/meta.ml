(** Meta information stored alongside the actual data chunks; includes: chunk size *)

open Sexplib.Std
open Util

module Private = struct
  module T = struct
    type t = {
      chunk_size: int; (* in bytes *)
    } [@@deriving sexp]
  end

  include T

  include Add_load_save_funs(T)
end

include Private

