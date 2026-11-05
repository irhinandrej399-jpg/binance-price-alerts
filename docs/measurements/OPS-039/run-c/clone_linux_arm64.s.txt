#include "textflag.h"
// Preserve child operands in registers: child uses a separate stack and no Go.
TEXT ·cloneControl(SB),NOSPLIT,$0-64
 MOVD nr+0(FP), R8
 MOVD fd+40(FP), R19
 MOVD word+48(FP), R20
 CMP $435, R8
 BEQ clone3
 MOVD flags+8(FP), R0
 MOVD stack+32(FP), R1
 MOVD $0, R2
 MOVD $0, R3
 MOVD $0, R4
 B invoke
clone3:
 MOVD args+16(FP), R0
 MOVD size+24(FP), R1
invoke:
 SVC $0
 CBZ R0, child
 MOVD R0, ret+56(FP)
 RET
child:
 MOVD R19, R0
 MOVD $57, R8
 SVC $0
 MOVD $42, R0
 MOVD R0, (R20)
 MOVD $0, R0
 MOVD $93, R8
 SVC $0
 B child
