;; As per https://www.w3.org/TR/wasm-core-1/#functions%E2%91%A0 functions may not specify more than
;; u32::MAX locals. Make sure this holds and does not break.


;; This is a validation failure
(assert_invalid
  (module binary
      "\00asm\01\00\00\00"    ;; the header
      "\01\04\01"             ;; 4 byte type section with 1 element
      "\60\00\00"             ;; fn() -> ()
      "\03\02\01"             ;; 2 byte func section with 1 element
      "\00"                   ;; signature 0
      "\0a\0a\01"             ;; 11 byte code section with 1 element
      "\08"                   ;; 4 bytes for this function
      "\01\ff\ff\ff\ff\0f\7f" ;; 1 local block containing 0xffff_ffff locals of type i32
      "\0b"                   ;; end
  )
  "locals exceed maximum"
)

;; Ensure that we don't hit any panics with > 0xFFFF_FFFF locals.
(assert_invalid
  (module binary
      "\00asm\01\00\00\00"    ;; the header
      "\01\04\01"             ;; 4 byte type section with 1 element
      "\60\00\00"             ;; fn() -> ()
      "\03\02\01"             ;; 2 byte func section with 1 element
      "\00"                   ;; signature 0
      "\0a\0c\01"             ;; 11 byte code section with 1 element
      "\0a"                   ;; 11 bytes for this function
      "\02"                   ;; 2 local blocks
      "\ff\ff\ff\ff\0f\7f"    ;; local block containing 0xffff_ffff locals of type i32
      "\7f\7f"                ;; local block containing 0x7f locals of type i32
      "\0b"                   ;; end
  )
  "locals exceed maximum"
)
