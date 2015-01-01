Nextop Overlord


Principles
==========

- 1..n overlords per access key
- overlords may run on any cloud platform/region
- overlords are self-contained: no dependency on external databases or systems
- overlords maintain their state until told to change via a control connection from the hyperlord
- hyperlords connect into the overlord and issue control commands. Examples: notify new overlord, set permissions, transition connections to another overlord.

