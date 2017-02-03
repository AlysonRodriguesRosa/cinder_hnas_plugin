HNAS Cinder Plugin
==================
Customized Tempest plugin structure to run our Cinder QA Methodology in HNAS
driver.

============
Installation
============
When Tempest runs, it will automatically discover the installed plugins.
So we just need to install the Python packages that contains the plugin.

Clone the repository and install the package from the src tree:

.. code-block:: bash

    $ cd hnas-cinder-plugin-tempest
    $ sudo pip install -e .
    
Using virtual environments (in progress)
----------------------------------------
If you run Tempest inside a virtualenv you have to ensure that the Python
package containing the plugin is installed in the venv too.

E.g: Installing the plugin in a Tempest (from Rally) venv:

.. code-block:: bash

    $ cd /opt/stack/rally
    $ ./install_rally.sh -d ~/.rally/scenario01
    $ . ~/.rally/scenario01/bin/activate
    $ ~/.rally/scenario01/bin/pip install -e ~/cinder_hnas_plugin/
    $ ~/.rally/scenario01/bin/pip list | grep cinder_hnas_plugin

====================
How to run the tests
====================
1. To validate that Tempest discovered the test in the plugin, you can run:

   .. code-block:: bash 

    $ testr list-tests | grep cinder_hnas_plugin
    

   This command will show your complete list of test cases inside the plugin.


2. To run all tests from plugin:

   .. code-block:: bash  
    
    $ testr run cinder_hnas_plugin

3. To run a specific test, for example, running test case 04:

    .. code-block:: bash

    $ testr run cinder_hnas_plugin.tests.scenario.test_hnas_sb.TestHNASSB.test_hnas_sb04

4. To run using rally:

   .. code-block:: bash  
    
    $ testr run --subunit smoke | subunit-2to1 | ./tools/colorizer.py
