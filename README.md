# rubydj-pyworker

A pure Python2.7+ worker for Ruby-based DelayedJobs.

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
### From Pypi:
    pip install rubydj-pyworker
### From Github branch:
    pip install git+https://github.com/rayyansys/pyworker.git@<branch_name>#egg=rubydj-pyworker

## Usage

The simplest usage is creating a worker and running it:

    from pyworker.worker import Worker

    dbstring = 'postgres://user:password@host:port/database'
    w = Worker(dbstring)
    w.run()

This will create a worker that continuously polls the specified database
and pick up jobs to run. Of course, no jobs will be recognized so one should
declare jobs before calling the worker.

Example job:

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

Once this example job is declared, the worker can recognize it and
will call its `run` method once it is picked up, optionally with the
specified hooks.

### Configuration

Before calling the `run` method on the worker, you have these
configuration options:

    # seconds between database polls (default 10)
    w.sleep_delay = 3

    # maximum attempts before marking the job as permanently failing (default 3)
    w.max_attempts = 5

    # maximum run time allowed for the job, before it expires (default 3600)
    w.max_run_time = 14400

    # queue names to poll from the datbase, comma separated (default: 'default')
    w.queue_names = 'queue1,queue2'

Youc an also provide a logger class (from `logging` module) to have full control on logging configuration:

    import logging
    
    logging.basicConfig()
    logger = logging.getLogger('pyworker')
    logger.setLevel(logging.INFO)

    w = Worker(dbstring, logger)
    w.run()

## Limitations

- Only supports Postgres databases
- No read ahead option, picks up one job at a time
- Assumes UTC timezone in the database
- No access to your Ruby classes, you should implement all your logic from scratch in Python
- Reads only raw attributes of jobs from the database (job table columns), no relations
- Assumes you only need to call the `run` method in your job with no arguments
- No unit tests

## Contribute

Install the code for development:

    git clone https://github.com/rayyansys/pyworker.git
    cd pyworker
    python setup.py develop

Do your changes, then send a pull request.

## Publish

### Using Python
1. Increment the version number in `setup.py`
1. Install twine: `pip install twine`. You may need to upgrade pip first: `pip install --upgrade pip`
1. Create the distribution files: `python setup.py sdist bdist_wheel`
1. Optionally upload to [Test PyPi](https://test.pypi.org/) as a dry run: `twine upload -r testpypi dist/*`. You will need a separate account there
1. Upload to [PyPi](https://pypi.org/): `twine upload dist/*`

### Using Docker
Increment the version as in the first step above then:

```bash
docker build . -t pyworker:0.1.0
docker run -it --rm pyworker:0.1.0
```

Enter your PyPi username and password when prompted.

## License

Released under the MIT License.
