# pypark
-----

### Prerequisite

Intsall the latest version of python and add it to the environment variable.

----
### setup
#### Using GUI environment variable
To run Pyspark we need to ass it to the PYTHONPATH
- Right-click on "This PC" or "My Computer" and select "Properties."
- Click on "Advanced system settings" on the left.
- In the "System Properties" window, click the "Environment Variables" button.
- In the "Environment Variables" window, under "User variables" or "System variables," find and select the "PYTHONPATH" variable (if it exists) and click the "Edit" button. If it doesn't exist, click the "New" button to create a new variable.
- In the "Edit Environment Variable" window, add the paths to your PYTHONPATH variable. Each path should be separated by a semicolon ;. For example:

- Varaible name:`PYHTONPATH`
- Variable value:`C:\tools\Spark\spark-3.5.0-bin-hadoop3\python;
C:\tools\Spark\spark-3.5.0-bin-hadoop3\python\lib\py4j-0.10.9.7-src.zip`

#### Using terminal
```
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"
```
Where SPARK_HOME should be the path where spark-3.5.0-bin-hadoop is located.

Make sure that the version under `${SPARK_HOME}/python/lib/` matches the filename of py4j or you will encounter `ModuleNotFoundError: No module named 'py4j'` while executing import pyspark.

For example, if the file under `${SPARK_HOME}/python/lib/`is `py4j-0.10.9.3-src.zip`, then the export PYTHONPATH statement above should be changed to
```
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.3-src.zip:$PYTHONPATH"
```
### Testing
Run jupyter notebbok from the terminal.

In the notebook run the following:

```
import pyspark
```
It should run successfully without `ImportError: No module named pyspark
`
```
pyspark.__file__
```
Output
```
'C:\\tools\\Spark\\spark-3.5.0-bin-hadoop3\\python\\pyspark\\__init__.py'
```