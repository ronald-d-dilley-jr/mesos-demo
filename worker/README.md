# worker

A simple worker which does nothing but sleep for a randomly determined amount of time.

### Environment Variables
| Variable   | Description |
| ---------- | ----------- |
| DEMO_WORKER_SEED | Seed to use for random generator.  (Default - Current time) |
| DEMO_WORKER_STANDARD_RANGE | Generate a random number from 1 to <value> |
| DEMO_WORKER_VARIATION_RANGE | Generate a random number from 1 to <value>.  Added to the standard range value. |
| DEMO_WORKER_VARIATION_PERCENTAGE | For this percentage of the time add the variation range result to the standard value. |
