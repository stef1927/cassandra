#  Testing and Coverage

There are two major sets of tests for Cassandra: unit tests and distributed tests, or dtests for short. The unit tests are part of the Cassandra repository; dtests are functional tests that are available at https://github.com/riptano/cassandra-dtest.

##  Running the Unit Tests

Run `ant test` from the top-level directory of your Cassandra checkout to run all unit tests. To run a specific test class, run `ant -Dtest.name=<ClassName>`. In this case, ClassName should not be fully qualified (like StorageProxyTest). To run a specific test method, run `ant testsome -Dtest.name=<ClassName> -Dtest.methods=<comma-separated list of method names>`. When using the testsome command, ClassName should be a fully qualified name (like org.apache.cassandra.service.StorageProxyTest).

You can also run tests in parallel: `ant test -Dtest.runners=4` and from IntelliJ IDEA or Eclipse, refer to [building Cassandra](building.md) for more details.

##  Running the dtests

The dtests use CCM to test a local cluster. If the following instructions don't seem to work, you may find more current instructions in the [dtest repository](https://github.com/riptano/cassandra-dtest).

Install CCM. You can do this with pip by running `pip install ccm`.

Install nosetests. With pip, this is `pip install nose`.

Install the Cassandra Python driver. This can be installed using `pip install git+git://github.com/datastax/python-driver@cassandra-test`. This installs a dedicated test branch driver for new features. If you're working primarily on older versions, `pip install cassandra-test` may be sufficient.

Clone the dtest repository:

```git clone https://github.com/riptano/cassandra-dtest.git cassandra-dtest```

Set `$CASSANDRA_DIR` to the location of your cassandra checkout. For example: `export CASSANDRA_DIR=/home/joe/cassandra`. Make sure you've already built Cassandra in this directory. You can build Cassandra by running ant, refer to [building Cassandra](building.md) for more details.

Run all tests by running nosetests from the dtest checkout. You can run a specific module like so: `nosetests cql_tests.py`. You can run a specific test method like this: `nosetests cql_tests.py:TestCQL.counters_test`.

If you encounter any failures, you can confirm whether or not they exist in upstream branches by checking to see if the failing tests or test classes are tagged with the known_failure decorator. This decorator is documented inline in the dtests. If a test that is known to fail passes, or a test that is not known to fail succeeds, you should check the linked JIRA ticket to see if you've introduced any detrimental changes to that branch. 

You can also see the latest status of tests in Jenkins [here](http://cassci.datastax.com/). Select the view for the release that you are using and see if any tests are currently failing.

## Running the code coverage task

Run a basic coverage report of unit tests using `ant codecoverage`.

Alternatively, run any test task with ant `jacoco-run -Dtaskname=some_test_taskname`. Run more test tasks in this fashion to push more coverage data onto the report in progress. Then manually build the report with `ant jacoco-report` (the code coverage task shown above does this automatically).

View the report at `build/jacoco/index.html`.

When done, clean up JaCoCo data so it does not confuse your next coverage report: `ant jacoco-cleanup`.

##  Continuous integration

Jenkins runs the Cassandra tests continuously: http://cassci.datastax.com/. For frequent contributors, this Jenkins is set up to build branches from their GitHub repositories. It is likely that your reviewer will use this Jenkins instance to run tests for your patch. If you've already submitted a few patches, and foresee submitting Cassandra patches frequently, you can ask on #cassandra-dev to add your Github account to Jenkins so you can run continuous integration on your Github branches before submitting a patch. This is especially important if you plan on working on large patches.