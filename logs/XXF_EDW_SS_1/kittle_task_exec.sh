#/bin/bash
##定时执行kettle任务,直接执行即可

#导入环境变量
source /etc/profile

#kettle脚本位置
kettle_path="/usr/local/kettle8.2/data-integration/kitchen.sh"

#当前脚本所在目录
dir_path=$(cd `dirname $0`; pwd)

#任务名字
task_name="start_execution.kjb"
#日志
task_log="Operation"


cd ${dir_path}

echo ${dir_path}
echo ${dir_path}/${task_log}

${kettle_path} -file=${task_name} /level Basic /logfile /home/kettle-tasks/Kettle_JOB_线上环境/kettle_log/Operation.log


