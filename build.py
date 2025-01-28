import os
from pybuilder.core import use_plugin, init

use_plugin("python.core")
use_plugin("pypi:pybuilder_pytest")
use_plugin("python.flake8")
use_plugin("pypi:pybuilder_pytest_coverage")
use_plugin("python.distutils")
name = "simple_test"
default_task = "publish"

@init
def set_properties(project):
  project.version = os.environ.get("build_number", "0.0.1")

  project.set_property('dir_source_main_python', 'src')
  project.set_property('dir_source_pytest_python', 'tests')
  project.set_property('dir_source_main_scripts', 'scripts')

