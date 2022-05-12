(lang dune 2.0)
(name chunked_file)

;; for opam
(authors "Tom Ridge <tom.j.ridge@googlemail.com>")
(maintainers "Tom Ridge <tom.j.ridge@googlemail.com>")
(source (github tomjridge/chunked_file))
(homepage https://github.com/tomjridge/chunked_file)
(documentation https://github.com/tomjridge/chunked_file) ;; FIXME
(generate_opam_files true)


(package 
 (name chunked_file)
 (synopsis "Like a normal file, but stored in fixed-size chunks")
 (depends unix)
)
