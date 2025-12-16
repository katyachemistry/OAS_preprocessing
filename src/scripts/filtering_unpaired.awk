#!/usr/bin/awk -f

BEGIN {
    FS = "\t"; OFS = ""

    total = 0
    fail_substr = 0
    fail_stop = 0
    fail_in_frame = 0
    fail_frameshift = 0
    fail_productive = 0
    fail_empty = 0
    fail_cdr3len = 0
    fail_shorter= 0
    fail_missing_cys = 0
    fail_invalid_aa = 0
    passed = 0
}

{   
    # assertion that the number of columns == 97
    if (NF != 97) {
        print "97 columns assertion failed",fname >> "logs/filtering_errors"
        _assert_exit = 1
        exit 1
    }

    # count +1 line for total lines
    total++

    # if locus != chain type, skip the row
    {chain_type = substr($9, 3, 1)}
    if ($2 != chain_type) { fail_substr++; next }

    # if stop codon, skip the row
    if ($3 != "F") { fail_stop++; next }

    # if not vj_in_frame, skip the row
    if ($4 != "T") { fail_in_frame++; next }

    # if v_frameshift, skip the row
    if ($5 != "F") { fail_frameshift++; next }

    # if not productive, skip the row
    if ($6 != "T") { fail_productive++; next }

    anarci_num = $(anarci_column - 1)
    gsub(/,/, "", anarci_num)
    gsub(/'/, "", anarci_num)

    # empty fields
    if ((chain_type == "H") && (anarci_num ~ /(fwh1: \{\}|fwh2: \{\}|fwh3: \{\}|fwh4: \{\}|cdrh1: \{\}|cdrh2: \{\}|cdrh3: \{\})/)) {
        fail_empty++
        next
    }

    if ((chain_type == "K") && (anarci_num ~ /(fwk1: \{\}|fwk2: \{\}|fwk3: \{\}|fwk4: \{\}|cdrk1: \{\}|cdrk2: \{\}|cdrk3: \{\})/)) {
        fail_empty++
        next
    }

    if ((chain_type == "L") && (anarci_num ~ /(fwl1: \{\}|fwl2: \{\}|fwl3: \{\}|fwl4: \{\}|cdrl1: \{\}|cdrl2: \{\}|cdrl3: \{\})/)) {
        fail_empty++
        next
    }

    # invalid symbols check
    cleaned_seq = ""
    pos = 1
    len = length(anarci_num)

    while (pos <= len && match(substr(anarci_num, pos), /: *([A-Z])/, m)) {
        cleaned_seq = cleaned_seq m[1]
        pos += RSTART + RLENGTH - 1
    }

    if (cleaned_seq ~ /[^ACDEFGHIKLMNPQRSTVWY]/) {
        fail_invalid_aa++
        next
    }

    # if cdr3 length > 37, skip the row 
    if (length($47) > 37) { fail_cdr3len++; next }

    # ANARCI_output filtering section
    {split($anarci_column,array,"|")}
    {deletions = array[2]; missing_cys = array[4]; shorter_than_imgt_defined = array[5]}

    # Shorter than defined (except FW1)
    if (shorter_than_imgt_defined ~ /(fw2|fw3|fw4|cdr1|cdr2|cdr3)/) {fail_shorter++; next }

    # Missing conserved cysteines
    if (missing_cys) {fail_missing_cys++; next}

    # has_empty = 0
    # for (i = 34; i <= 47; i++) {
    #     if ($i == "") { has_empty = 1; break }
    # }
    # if (has_empty) { fail_empty++; next }

    #check_fields = "35 37 39 41 43 45 47"
    #split(check_fields, f_arr, " ")
    #for (j in f_arr) {
    #   field = f_arr[j]
    #   aa_seq = toupper($(field))
    #    if (aa_seq ~ /[^ACDEFGHIKLMNPQRSTVWY]/) {
    #        fail_invalid_aa++; next
    #    }
    #}

    # Deletions in FW1
    gsub(/^[ \t]+|[ \t]+$/, "", deletions)
    if ((shorter_than_imgt_defined ~ /fw1/) && deletions) {
        match(deletions, /Deletions: *([0-9, ]+)/, m)
        deletions = m[1]
        gsub(/, */, " ", deletions)
    } else {
        deletions = ""
    }

    # CANCEL if ( deletions && ! \
    #    ( \
    #        (deletions == "Deletions: 10, 73" || deletions == "Deletions: 73" || deletions == "Deletions: 10") || \
    #        (chain_type == "L" && (deletions == "Deletions: 10, 73, 81, 82" || deletions == "Deletions: 10, 81, 82")) || \
    #        (chain_type == "K" && (deletions == "Deletions: 73, 81, 82" || deletions == "Deletions: 81, 82")) \
    #    ) \
    # ) { fail_deletions++; next }

    passed++

    print bsource","btype","isotype","$9","$10","$11","chain_type","deletions","anarci_num

}

END {
    if (_assert_exit)
        exit 1
    split(fname,path_array,"/")
    logfile = "logs/filtering_log_" path_array[length(path_array)]
    print fname "\nTotal lines processed: ", total >> logfile
    print "Failed on locus != chain type: ", fail_substr >> logfile
    print "Failed on stop-codon: ", fail_stop >> logfile
    print "Failed on vj_in_frame: ", fail_in_frame >> logfile
    print "Failed on v_frameshift: ", fail_frameshift >> logfile
    print "Failed on productive: ", fail_productive >> logfile
    print "Failed on empty fragments fields (34–47): ", fail_empty >> logfile
    print "Failed on invalid amino acids symbols: ", fail_invalid_aa >> logfile
    print "Failed on CDR3 length more than 37: ", fail_cdr3len >> logfile
    print "Failed on shorter than IMGT-defined (except for FW1): ", fail_shorter >> logfile
    print "Failed on missing conserved cysteines: ", fail_missing_cys >> logfile
    print "Passed all filters: ", passed, "\n" >> logfile
    close(logfile)
}
