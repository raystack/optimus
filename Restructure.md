### Code structure

#### Optimus
- main.go
- client - will contain client side cmds, extensions & local repo
- server
- protos
- internal - everything which is shared with in optimus.
  - store
  - lib
  - telemetry - everything around metrics/traces/logs
    - dashboards
- core
  - tenant
  - job
  - resource
  - schedule
- sdk - will contain plugin interface code.
- ext - can be renamed to adapters later

#### Bounded Context Structure
- all domain models in the root
- dto
- handler
- service

### Developer Guidelines
- Use of Consumer Interfaces
- Use names instead of ids.
- Pass domain object as pointers
- When using string as identifier/business logic around it, need to express as business concept
- Domain objects expose only behaviour, not state
- Value objects are immutable

