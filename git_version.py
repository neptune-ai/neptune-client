# Original source: https://github.com/Changaco/version.py
import os
import re
from distutils.cmd import Command
from subprocess import CalledProcessError, call, check_output

PREFIX = ''
INITIAL_VERSION = '0.0.0'

tag_re = re.compile(r'\btag: %s([0-9][^,]*)\b' % PREFIX)
version_re = re.compile('^Version: (.+)$', re.M)


def get_git_version():
    if "VERSION" in os.environ:
        return os.environ.get("VERSION")

    # Return the version if it has been injected into the file by git-archive
    version = tag_re.search('$Format:%D$')
    if version:
        return version.group(1)

    # Get the version using "git describe".
    try:
        cmd = 'git describe --tags --match %s[0-9.]* --dirty' % PREFIX
        version = check_output(cmd.split()).decode().strip()[len(PREFIX):]
    except CalledProcessError:
        version = INITIAL_VERSION + '+' + check_output('git rev-parse HEAD'.split()).decode().strip()[:10]
        if call('git diff --quiet'.split()) != 0:
            version += '.dirty'

    # PEP 440 compatibility
    if '-' in version:
        version_tokens = version.split('-')
        version = version_tokens[0] + '+' + '.'.join(version_tokens[1:])

    return version


class GitVersion(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        version = get_git_version()
        self.distribution.metadata.version = version

        with open('VERSION', 'w') as version_file:
            version_file.write(version)
