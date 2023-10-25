# Suppressions for Clang Sanitizers #

This folder contains [supression files](https://clang.llvm.org/docs/SanitizerSpecialCaseList.html) for
running lantern using Clang's [AddressSanitizer](https://clang.llvm.org/docs/AddressSanitizer.html)
and [UndefinedBehaviorSanitizer](https://clang.llvm.org/docs/UndefinedBehaviorSanitizer.html), taken from timescale's regression suite. 

There are a few places system libraries have UB and where postgres has benign memory leaks, in order to run these sanitizers, we suppress these warning.
