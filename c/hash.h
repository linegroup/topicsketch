//
//  hash.h
//  The multilinear family hash function
//  hash max 32-length string
//

#ifndef hash_h
#define hash_h

#include <stdint.h>
#include <stdlib.h>

uint32_t initialize(uint32_t H); // H: the number of groups

uint32_t hash(char * str, uint32_t len, uint32_t h);

#endif /* hash_h */
