.. include :: banner.rst

Support
=======

If you are having issues, or questions, feel free to  check out the :ref:`FAQ` below or search the `issues on the RedGrease GitHub repository <https://github.com/lyngon/redgrease/issues>`_.

If you can't find a satisfactory answer, feel to post it as an "issue" in RedGrease GitHub repository:

* `Post a question <https://github.com/lyngon/redgrease/issues/new?assignees=d00astro&labels=documentation%2C+question&template=question.md&title=%5BQUESTION%5D>`_
* `Request a new feature <https://github.com/lyngon/redgrease/issues/new?assignees=d00astro&labels=feature&template=new-feature-request.md&title=%5BFEATURE%5D>`_

These will be addressed on a best-effort basis.


Professional Support
--------------------

RedGrease is backed by `Lyngon Pte. Ltd. <https://www.lyngon.com>`_ and professional support with SLA's can be provided on request. For inquiries please send a mail to `support@lyngon.com <mailto:support@lyngon.com>`_. 


FAQ
---

.. _faq_license:

Q: Can I use RedGrease for Commercial Applications?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
A: Yes

RedGrease is licensed under the `MIT <https://mit-license.org/>`_ license, which is a very permissive licence and allows for commercial use. The same goes for Open Source Redis, which is licensed under the `3-Clause-BSD <https://opensource.org/licenses/BSD-3-Clause>`_ licence, and is similarly permissive. However, the RedisGears module is licensed under a custom `Redis Source Available License (RSAL) <https://redislabs.com/wp-content/uploads/2019/09/redis-source-available-license.pdf>`_, which limits usage to "non-database products". 



.. _faq_elasticache:

Q: Can I run Redis Gears / RedGrease on AWS ElastiCache?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
A: No

Unfortunately AWS ElastiCache does not (yet) support Redis modules, including Redis Gears.



.. _faq_spelling:

Q: Why are there so many spelling mistakes?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
A: The author suffers from mild `dyslexia <https://en.wikipedia.org/wiki/Dyslexia>`_ and has a had time spotting when a word isn't right.

Hopefully the sub-par spelling is not indicative of the quality of the software.


.. _issues:

Reporting issues
----------------

* `Report a bug <https://github.com/lyngon/redgrease/issues/new?assignees=d00astro&labels=bug&template=bug_report.md&title=%5BBUG%5D>`_


.. _contribute:

Contribute
==========

Feel free to submit PRs.


.. _dev_setup:

Development Setup
-----------------

After cloning or forking the repository, it is recommended to do the following:

#. In the project root, create and activate a Python 3.7 virtual environment. E.g:

    .. code-block:: console

        cd redgrease

        virtualenv -p python3.7 .venv 

        source .venv/bin/activate

#. Install the Development, Test **and** all package requirements:

    .. code-block:: console

        pip install -r src/requirements-dev.txt

#. Install `redgrease` in "develop" mode:

    .. code-block:: console

        pip install -e .

#. [optional] If you want to build the docs:

    .. code-block:: console

        pip install -r docs/source/requirements.txt

.. note::
    
    It is highly recommended to check / lint the code regularly (continuously) with::

        black src/
        flake8 src/
        isort src/
        mypy src/

.. _local_testing:

Local Testing
-------------

.. include :: wip.rst

To run the test, docker needs to be installed as it is used to spin up clean Redis instances.

.. include :: footer.rst