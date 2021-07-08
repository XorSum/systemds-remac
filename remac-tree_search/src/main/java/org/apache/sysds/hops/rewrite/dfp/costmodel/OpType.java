package org.apache.sysds.hops.rewrite.dfp.costmodel;

enum OpType {
    READ, WRITE,
    SP_PLUS, SP_SUB, SP_DIV, SP_MULT, SP_MM, SP_TRANS,
    CP_PLUS, CP_SUB, CP_DIV, CP_MULT, CP_MM, CP_TRANS
}
