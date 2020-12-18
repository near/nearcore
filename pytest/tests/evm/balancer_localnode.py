print('test')

from rc import bash
p = bash('''
set -x
pwd
ls -al
cd
ls -al
cat ~/.bashrc
echo $PATH
''')
print(p.stdout)
print(p.stderr)