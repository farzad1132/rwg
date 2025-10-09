# RWG – Research workload Generator

RWG is a minimal but fast **open loop** workload generator designed for flexibility, extensibility, and scalability in mind that is required for research setup.

# Key features
- Supporting different inter-arrival distributions for workload generation (e.g., fixed and exponential)
- Supporting multiple levels for offered rate with adjustable durations
- Low overhead scaling for high load rates
- Comprehensive metric collection and built-in parser to generate output files for End of Test and Realtime reports.
- Supporting both HTTP/1 and gRPC (HTTP/2)

# TODO

- [ ] Add capability to replay traces
- [ ] Add bimodal inter-arrival distribution 
- [ ] Add dynamic stib generation using `reflection` as opposed to relying on static stubs