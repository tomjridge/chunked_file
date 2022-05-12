(** A chunked file is like a normal file, but the data is stored in fixed-size chunks.


Invariants:

- All but the last chunk are fully of size chunk_size, or absent altogether (and it is an
   error to read from these gaps)

 *)

open Util
open Meta

type t = { 
  root: string;
  chunk_size: int;
  chunk_fds: (int,Unix.file_descr) Hashtbl.t;
  (* NOTE we don't currently make any effort to expunge unused fds; if the chunk size is
     reasonably large (eg 1GB) and the overall file data is some small multiple (say,
     100GB), then we need at max 100 fds, which is not so much; obviously there is more
     work to do if you want to keep the number of fds in use small *)
  mutable truncate_before_pos: int; 
  (* [truncate_before_pos] is the last pos argument when calling [truncate_before]; this
     is mono increasing (it is an error to call truncate_before x; truncate_before y; when
     y<x *)
  mutable sz : int; 
  (* [sz] is the file size (the size of the virtual file being simulated) *)
}

let init_chunk_fds () = Hashtbl.create 100

(* fn is the base name for the chunked file; it should not exist initially; a directory
   will be created, within which will be stored the meta file and the data files *)
let create ~fn ~chunk_size =
  assert(not(Sys.file_exists fn));
  assert(chunk_size > 0); (* really, we want it to be much bigger for perf reasons *)
  Sys.mkdir fn 0o777;
  let root = fn in
  let meta = { chunk_size } in
  Meta.save meta Fn.(root / "meta");
  { root; chunk_size; chunk_fds=init_chunk_fds() }

let open_ ~fn = 
  let root = fn in
  assert(Sys.file_exists fn);
  assert(Sys.is_directory fn);
  assert(Sys.file_exists Fn.(root / "meta"));
  let meta = Meta.load Fn.(root / "meta") in
  { root; chunk_size=meta.chunk_size; chunk_fds=init_chunk_fds() }

let close t = 
  t.chunk_fds |> Hashtbl.iter (fun _ fd -> 
      close_no_err fd);
  Hashtbl.clear t.chunk_fds;
  ()
(* the t value should not be used afterwards, although it could be... *)

let get_fd_for_chunk t chunk =
  Hashtbl.find_opt t.chunk_fds chunk |> function
  | Some fd -> fd
  | None -> (
      (* FIXME need to check if chunk is missing; if so, it is an error to try to access
         it *)
      let fd = File.open_ ~path:Fn.(t.root / "data."^(string_of_int chunk)) in
      Hashtbl.replace t.chunk_fds chunk fd;
      fd)

(** [write_to_chunk t ~chunk ~chunk_off ~buf] writes the data in buf to the chunk at the
   given offset within the chunk; the data must fit within the chunk, i.e. chunk_off+|buf|
   <= chunk_size *)
let write_to_chunk t ~chunk ~chunk_off ~buf = 
  assert(chunk_off+Bytes.length buf <= t.chunk_size);
  let fd = get_fd_for_chunk t chunk in
  File.pwrite fd ~off:(ref chunk_off) buf;
  ()

let read_from_chunk t ~chunk ~chunk_off ~buf ~len =
  assert(len <= Bytes.length buf);
  assert(chunk_off + len <= t.chunk_size);
  let fd = get_fd_for_chunk t chunk in
  let n = File.pread fd ~off:(ref chunk_off) ~len ~buf:(Bytes.sub buf 0 len) in
  (* NOTE n can be less than len in case the chunk is the last chunk and there is not
     enough data *)
  n                                            

(** [pwrite ~t ~off buf] writes the bytes in [buf] to the file at the position given by
   [off]; [off] is an int ref which gets updated *)
let pwrite t ~(off:int ref) buf =
  let off' = !off in
  let chsz = t.chunk_size in
  let chunk,chunk_off = (off'/chsz), (off' mod chsz) in
  (* write to the individual chunks *)
  (chunk,chunk_off,buf) |> iter_k (fun ~k (chunk,chunk_off,buf) -> 
      let buf_len = Bytes.length buf in
      match buf_len with
      | 0 -> ()
      | _ -> 
        let can_write = min (chsz - chunk_off) buf_len in
        write_to_chunk t ~chunk ~chunk_off ~buf:(Bytes.sub buf 0 can_write);
        k (chunk+1,0,Bytes.sub buf can_write (buf_len - can_write)));
  off := !off + Bytes.length buf;
  ()

(* FIXME invariant: all chunks but the last are chunk_size in length; so if we pwrite
   beyond end of file, we need all other chunks previous to be a full chunk_size; so we
   need to track the size of the file, and fill out chunks;

except that we also want to allow chunks to be completely absent, provided we never try to
   read from them

*)

let pread t ~off ~len ~buf =
  assert(len <= Bytes.length buf);
  let off' = !off in
  let chsz = t.chunk_size in
  let chunk,chunk_off = (off'/chsz), (off' mod chsz) in
  (* read from the individual chunks *)
  let n = 
    let len_sofar,buf_off,len_rem = 0,0,len in
    (chunk,chunk_off,buf_off,len_sofar,len_rem) |> iter_k (fun ~k (chunk,chunk_off,buf_off,len_sofar,len_rem) ->
        match len_rem with
        | 0 -> len_sofar
        | _ -> 
          let can_read = min (chsz - chunk_off) len_rem in
          let n = read_from_chunk t ~chunk ~chunk_off ~len:can_read ~buf:(Bytes.sub buf buf_off can_read) in
          match n = can_read with
          | true -> k (chunk+1,0,buf_off+n,len_sofar+n,len_rem-n)
          | false -> len_sofar)
  in            
  off := !off + n;
  ()

(** [truncate_before t ~pos] keeps the chunk containing pos, and all later chunks, but
   removes earlier chunks *)
let truncate_before t ~pos = ()
  
(* NOTE this requires us to record the size of the file *)
let append t s = ()






