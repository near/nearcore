print('test')

from rc import bash
bash('''
set -x
pwd
ls -al
cd
ls -al
cat ~/.bashrc
echo $PATH
''')