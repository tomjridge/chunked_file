(** Essentially the Y combinator; useful for anonymous recursive
    functions. The k argument is the recursive callExample:

    {[
      iter_k (fun ~k n -> 
          if n = 0 then 1 else n * k (n-1))

    ]}


*)
let iter_k f (x:'a) =
  let rec k x = f ~k x in
  k x  


module Add_load_save_funs(S:sig type t[@@deriving sexp] end) = struct
  open S

  let save t fn = Sexplib.Sexp.save_hum fn (t |> sexp_of_t)

  let load fn = Sexplib.Sexp.load_sexp fn |> t_of_sexp

  let to_string t = Sexplib.Sexp.to_string_hum (t |> sexp_of_t)
  
  (* this loads from the file name s! *)
  let _of_string s = Sexplib.Sexp.load_sexp s |> t_of_sexp
  let of_string s = Sexplib.Sexp.of_string s |> t_of_sexp
      
  let to_bytes t = t |> to_string |> Bytes.unsafe_of_string

  let of_bytes bs = bs |> Bytes.unsafe_to_string |> of_string

end

module Fn = struct
  let ( / ) a b = a ^ "/" ^ b
end

let close_no_err (fd:Unix.file_descr) = 
  try
    Unix.close fd
  with _ -> ()


module Pwrite = struct
  type t = {
    pwrite: off:int ref -> bytes -> unit;
  }
end

module Pread = struct
  type t = {
    pread : off:int ref -> len:int -> buf:bytes -> int; 
  }
end

module File = struct
  let create ~path =
    let ok = not (Sys.file_exists path) in
    assert(ok);
    let fd = Unix.(openfile path [O_CREAT;O_RDWR;O_EXCL;O_CLOEXEC] 0o660) in
    fd

  let open_ ~path =
    let ok = Sys.file_exists path in
    assert(ok);
    let fd = Unix.(openfile path [O_RDWR;O_CLOEXEC] 0o660) in
    fd

  let pread fd ~off ~len ~buf =
    assert(len <= Bytes.length buf);
    ignore(Unix.lseek fd !off SEEK_SET);    
    let pos =
      0 |> iter_k (fun ~k pos -> 
          match pos=len with 
          | true -> pos
          | false -> 
            let n = Unix.read fd buf pos (len - pos) in
            if n = 0 then pos else k (pos+n))
    in
    off := !off + pos;
    pos   

  let read_string ~fd ~off ~len = 
    let buf = Bytes.create len in
    let n = pread fd ~off ~len ~buf in
    Bytes.unsafe_to_string (Bytes.sub buf 0 n)

  let pwrite fd ~off buf =
    ignore(Unix.lseek fd !off SEEK_SET);    
    let len = Bytes.length buf in
    let n = Unix.write fd buf 0 len in
    assert(n=len);
    off := !off + n;
    ()    

  let size fn = Unix.((stat fn).st_size)

  let copy ~(src:Pread.t) ~(dst:Pwrite.t) ~src_off ~len ~dst_off = 
    match len = 0 with
    | true -> ()
    | false -> 
      let Pread.{pread},Pwrite.{pwrite} = src,dst in
      let src_off = ref src_off in
      let dst_off = ref dst_off in
      let buf_sz = 8192 in
      let buf = Bytes.create buf_sz in
      len |> iter_k (fun ~k len -> 
          match len <=0 with
          | true -> ()
          | false -> 
            let n = pread ~off:src_off ~len:(min buf_sz len) ~buf in
            (* (if n=0 then Log.warn (fun m -> m "pread returned n=0 bytes, off=%d len=%d"
               !src_off (min buf_sz len))); *)
            assert(n>0);
            pwrite ~off:dst_off (Bytes.sub buf 0 n);
            k (len - n))
end
