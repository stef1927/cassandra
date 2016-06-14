# Contributing to Cassandra

## Getting in touch

Refer to [this page](../getting_started/contact_us.md) for instructions on how to sign up to the developers mailing list or `#cassandra-dev` IRC channel.

## Not a developer?

You can still contribute by reporting bugs, testing submitted patches and recent releases, replying to questions on the mailing lists, IRC channels and Stackoverflow, updating or reporting problems with this documentation and more. Get in touch with us if you have any suggestions or feedback.

## Contributing a patch

Patch development for Cassandra is coordinated via the Apache hosted JIRA server available [here](https://issues.apache.org/jira/browse/CASSANDRA/?selectedTab=com.atlassian.jira.jira-projects-plugin:summary-panel). Create an account if you don't already have one.

Send an email to the developers mailing list or ask on #cassandra-dev to add your JIRA username to the "jira-assignable" group. This ensures that JIRA tickets can be assigned to your username.

Pick a JIRA ticket to work on. Some possibilities are marked with the low-hanging fruit label `lhf` in JIRA, see [here](https://issues.apache.org/jira/issues/?jql=project%20%3D%2012310865%20AND%20labels%20%3D%20lhf%20AND%20status%20!%3D%20resolved). You should pick a ticket that is in the OPEN status and not assigned to anyone. If a ticket is already assigned, check with the person the ticket is currently assigned to, to make sure no work has been done yet and that it is OK for you to take over. You can also create a new ticket, but make sure that new feature tickets get a +1 before starting work on it. Send an email to the developer's mailing list or ping us in #cassandra-dev if no one replies to your new feature suggestion within a couple of days. If you are sure you have found a bug you can go ahead and provide a patch for it.

Read the relevant documentation pages, especially the architecture pages.

When you're ready to start working on the ticket, if it has not yet been assigned, assign the ticket to yourself in JIRA. This is done using the "Assign" button at the top of the ticket. Move the ticket to "In progress" by selecting Workflow -> Start Progress.

Refer to [building Cassandra](building.md) for setting up your environment.

Make sure to follow our [coding style](https://wiki.apache.org/cassandra/CodeStyle).

Make sure to write appropriate tests for your patch (unit or dtests), refer to [testing](testing.md).

Consider going through the [Review Checklist](https://wiki.apache.org/cassandra/HowToReview) for your code. This will help you to understand how others will consider your change for inclusion. 

To make life easier for your reviewer/committer, you may want to make sure your patch applies cleanly to later branches and create additional patches/branches for later Cassandra versions to which your original patch does not apply cleanly. That said, this is not critical, and you will receive feedback on your patch regardless. 

Submit your patch to the JIRA ticket in one of these two ways:

* Put a link to your Github branch, if you have a Github account and a fork of Cassandra on it.

* Attach a patch to the ticket, which can be created via `git format-patch`.

The first method is preferable but presents a problem if the branch is deleted later on. Therefore remember to attach a patch before deleting the branch, after the patch has been committed. Ideally, a few weeks after the patch has been committed, so that in case of problems the original Github branch is still available.

Set the status of the ticket to "Patch available" by clicking "Submit Patch" at the top of the ticket. If no reviewer is assigned within a couple of days, ping us on the developer's mailing list or #cassandra-dev.

The reviewer will import your patch into their Github repository in order to run continuous integration tests, as explained in [testing](testing.md). Any problems will be reported on the ticket, along with any review comments.

If the reviewer has given feedback to improve the patch, make the necessary changes and move the ticket into "Patch Available" once again.

Once the review is complete, the patch will be up-merged to more recent branches, if applicable, and the continuous integration tests will be repeated on all branches the patch has been applied to. Once all tests are fine, you will receive a +1 and the ticket will be marked as "Ready to Commit". Eventually it will be committed by one of the committers, using your Github username as the author.

##  Bundled Drivers

A copy of the [Python driver](https://github.com/datastax/python-driver) is included for use in cqlsh. For instructions on how to package the bundled driver for the Cassandra project, see the instructions [here](https://github.com/datastax/python-driver/blob/master/README-dev.rst#packaging-for-cassandra).
