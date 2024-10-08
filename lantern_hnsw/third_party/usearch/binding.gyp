{
    "targets": [
        {
            "target_name": "usearch",
            "sources": ["javascript/lib.cpp"],
            "cflags": [
                "-fexceptions",
                "-Wno-unknown-pragmas",
                "-Wno-maybe-uninitialized",
            ],
            "cflags_cc": [
                "-fexceptions",
                "-Wno-unknown-pragmas",
                "-Wno-maybe-uninitialized",
                "-std=c++17",
            ],
            "include_dirs": [
                "<!@(node -p \"require('node-addon-api').include\")",
                "include",
                "fp16/include",
                "simsimd/include",
            ],
            "dependencies": ["<!(node -p \"require('node-addon-api').gyp\")"],
            "xcode_settings": {
                "GCC_ENABLE_CPP_EXCEPTIONS": "YES",
                "CLANG_CXX_LIBRARY": "libc++",
                "MACOSX_DEPLOYMENT_TARGET": "10.15",
            },
            "msvs_settings": {
                "VCCLCompilerTool": {
                    "ExceptionHandling": 1,
                    "AdditionalOptions": ["-std:c++17"],
                }
            },
            "conditions": [
                [
                    'OS=="linux"',
                    {
                        "cflags_cc": [
                            '<!(if [ "$USEARCH_USE_OPENMP" = "1" ]; then echo \'-fopenmp\'; fi)',
                        ],
                        "ldflags": [
                            '<!(if [ "$USEARCH_USE_OPENMP" = "1" ]; then echo \'-lgomp\'; fi)'
                        ],
                        "defines": [
                            "USEARCH_USE_OPENMP=<!(echo ${USEARCH_USE_OPENMP:-0})",
                            "USEARCH_USE_SIMSIMD=<!(echo ${USEARCH_USE_SIMSIMD:-1})",
                            "USEARCH_USE_FP16LIB=<!(echo ${USEARCH_USE_FP16LIB:-1})",
                            "SIMSIMD_TARGET_X86_AVX512=<!(echo ${SIMSIMD_TARGET_X86_AVX512:-1})",
                            "SIMSIMD_TARGET_ARM_SVE=<!(echo ${SIMSIMD_TARGET_ARM_SVE:-1})",
                            "SIMSIMD_TARGET_X86_AVX2=<!(echo ${SIMSIMD_TARGET_X86_AVX2:-1})",
                            "SIMSIMD_TARGET_ARM_NEON=<!(echo ${SIMSIMD_TARGET_ARM_NEON:-1})",
                        ],
                    },
                ],
                [
                    'OS=="mac"',
                    {
                        "defines": [
                            "USEARCH_USE_OPENMP=<!(echo ${USEARCH_USE_OPENMP:-0})",
                            "USEARCH_USE_SIMSIMD=<!(echo ${USEARCH_USE_SIMSIMD:-0})",
                            "USEARCH_USE_FP16LIB=<!(echo ${USEARCH_USE_FP16LIB:-1})",
                            "SIMSIMD_TARGET_X86_AVX512=<!(echo ${SIMSIMD_TARGET_X86_AVX512:-0})",
                            "SIMSIMD_TARGET_ARM_SVE=<!(echo ${SIMSIMD_TARGET_ARM_SVE:-0})",
                            "SIMSIMD_TARGET_X86_AVX2=<!(echo ${SIMSIMD_TARGET_X86_AVX2:-1})",
                            "SIMSIMD_TARGET_ARM_NEON=<!(echo ${SIMSIMD_TARGET_ARM_NEON:-1})",
                        ],
                    },
                ],
                [
                    'OS=="win"',
                    {
                        "defines": [
                            "USEARCH_USE_OPENMP=0",
                            "USEARCH_USE_SIMSIMD=0",
                            "USEARCH_USE_FP16LIB=1",
                            "SIMSIMD_TARGET_X86_AVX512=0",
                            "SIMSIMD_TARGET_ARM_SVE=0",
                            "SIMSIMD_TARGET_X86_AVX2=0",
                            "SIMSIMD_TARGET_ARM_NEON=0",
                        ],
                    },
                ],
            ],
        }
    ]
}
