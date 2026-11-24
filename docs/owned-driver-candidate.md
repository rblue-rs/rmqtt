# Historical private driver candidate

The earlier cfg(test)-only candidate is superseded by the actual production actor
integration described in [owned-driver.md](owned-driver.md). Its private state,
allocator and broker tests are supplementary evidence, not substitutes for public
Client/C/Dart paths. The upstream EventLoop reference probes are still separate.
No aggregate process/RSS cap or application exactly-once follows from those tests.
