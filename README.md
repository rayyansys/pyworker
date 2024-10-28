# rubydj-pyworker

A pure Python 3.x worker for Ruby-based DelayedJobs.
Note that Python 2.7 is not supported.

## Why rubydj-pyworker?

If you have a scientifc [Ruby on Rails](http://rubyonrails.org/) application,
you may find yourself requiring background jobs to run in Python
to leverage all the scientific libraries that Python is famous for.
So instead of creating wrapper Ruby jobs that do `system` calls to Python,
why not implement the whole job in Python and launch a Python worker
to dispatch only those jobs?
Also, with the microservice architecture and containers world,
it is not practical to deploy a fat container with both Ruby and Python
installed. It is much more cleaner to deploy 2 separate containers.

pyworkers offers you the `Worker` class that does all the background job
dispatching, scheduling and error handling, plus a `Job` class that you
should subclass to implement your pure python jobs. It basically replicates
the [delayed_job](https://github.com/collectiveidea/delayed_job) Ruby gem behavior.

## Installation

### From PyPI

    pip install rubydj-pyworker

You can also pick up any version from [PyPI](https://pypi.org/project/rubydj-pyworker/)
that supports Python 3.x (>= 1.0.0).

### From Github branch

    pip install git+https://github.com/rayyansys/pyworker.git@<branch_name>#egg=rubydj-pyworker

## Usage

The simplest usage is creating a worker and running it:

```python
from pyworker.worker import Worker

dbstring = 'postgres://user:password@host:port/database'
w = Worker(dbstring)
w.run()
```

This will create a worker that continuously polls the specified database
and pick up jobs to run. Of course, no jobs will be recognized so one should
declare jobs before calling the worker.

Example job:

```python
from pyworker.job import Job

class MyJob(Job):
    def __init__(self, *args, **kwargs):
        super(MyJob, self).__init__(*args, **kwargs)

    def run(self):
        self.logger.info("Running MyJob.run")

    # optional
    def before(self):
        self.logger.debug("Running MyJob.before hook")

    # optional
    def after(self):
        self.logger.debug("Running MyJob.after hook")

    # optional
    def success(self):
        self.logger.debug("Running MyJob.success hook")

    # optional
    def error(self):
        self.logger.debug("Running MyJob.error hook")

    # optional
    def failure(self):
        self.logger.debug("Running MyJob.failure hook")
```

Once this example job is declared, the worker can recognize it and
will call its `run` method once it is picked up, optionally with the
specified hooks.

### Configuration

Before calling the `run` method on the worker, you have these
configuration options:

```python
# seconds between database polls (default 10)
w.sleep_delay = 3

# maximum attempts before marking the job as permanently failing (default 3)
w.max_attempts = 5

# maximum run time allowed for the job, before it expires (default 3600)
w.max_run_time = 14400

# queue names to poll from the datbase, comma separated (default: 'default')
w.queue_names = 'queue1,queue2'
```

You can also provide a logger class (from `logging` module) to have full control on logging configuration:

```python
import logging

logging.basicConfig()
logger = logging.getLogger('pyworker')
logger.setLevel(logging.INFO)

w = Worker(dbstring, logger)
w.run()
```

## Monitoring

Workers can be monitored using [New Relic](https://newrelic.com/). All you need
to do is to create a free account there, then add the following to your
environment variables:

```bash
NEW_RELIC_LICENSE_KEY=<your_newrelic_license_key>
NEW_RELIC_APP_NAME=<your_newrelic_app_name>
```

All jobs will be reported under the `BackgroundTask` category. This includes
standard metrics like throughput, response time (job duration), error rate, etc.
Additional transaction custom attributes are also reported out of the box:

1. `jobName`: the name of the job class
1. `jobId`: the id of the delayed job in the database
1. `jobQueue`: the queue name of the job
1. `jobAttempts`: the number of attempts for the job
1. `jobLatency`: the time between the job creation and the time it was picked up by the worker

If you wish to automatically report additional attributes from the delayed job table
(e.g. the priority of the job or any other custom fields), you can do so by
providing a list of fields to the worker:

```python
worker = Worker(dbstring,
    logger=logger,
    extra_delayed_job_fields=['priority', 'custom_field'])
```

Columns of types `string`/`text`, `int`, `float` and `bool` are reported as is.
`json`/`jsonb` types are expanded into separate attributes. All other fields are converted
to JSON strings.

You can also automatically prefix all attributes with a string before reporting:

```python
worker = Worker(dbstring,
    logger=logger,
    reported_attributes_prefix='myapp.')
```

If you wish to report additional custom attributes from your job, you can do so
by calling the `reporter` object that is available in the job instance:

```python
class MyJob(Job):
    def __init__(self, *args, **kwargs):
        super(MyJob, self).__init__(*args, **kwargs)

    def run(self):
        ...
        self.reporter.report(
            my_custom_attribute='my_custom_value',
            another_custom_attribute='another_custom_value',
            ...)
        ...
```

The prefix will be applied to all attributes reported from the job as well
as the camel case conversion. If you wish to skip both, you can use the
`report_raw` function instead of `report`.

In all cases, two additional attributes are reported:

1. `error`: a Boolean indicating whether the job has failed or not
1. `jobFailure`: a Boolean indicating whether the job has permanently failed or not

The `error` attribute is reported as is, i.e. without prefix or camel case conversion.

Additionally, the end user id is reported if the custom attributes contain the id
in any of the below formats:

1. `user_id` (with and without prefix)
1. `userId` (with and without prefix)

This is useful in identifying impacted users count in case of job errors.

## Limitations

- Only supports Postgres databases
- No read ahead option, picks up one job at a time
- Assumes UTC timezone in the database
- No access to your Ruby classes, you should implement all your logic from scratch in Python
- Reads only raw attributes of jobs from the database (job table columns), no relations
- Assumes you only need to call the `run` method in your job with no arguments

## Contribute

Install the code for development:

    git clone https://github.com/rayyansys/pyworker.git
    cd pyworker
    python setup.py develop

Do your changes, then send a pull request.

## Test

    pip install -r requirements-test.txt
    pytest

## Publish

1. Increment the version number in `setup.py`
1. Install `twine` and `wheel`: `pip install twine wheel`. You may need to upgrade pip first: `pip install --upgrade pip`
1. Create the distribution files: `python setup.py sdist bdist_wheel`
1. Optionally upload to [Test PyPI](https://test.pypi.org/) as a dry run: `twine upload -r testpypi dist/*`. You will need a separate account there
1. Upload to [PyPI](https://pypi.org/): `twine upload dist/*`

Enter your PyPI username and password when prompted.

## License

Released under the MIT License.
