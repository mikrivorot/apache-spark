### Start: Install all required packages

```bash
python3 -m venv venv

source venv/bin/activate

brew install openjdk@17
brew install scala
brew install apache-spark

spark-sumbit test_1.py (/usr/local/Cellar/apache-spark/3.5.3/bin/spark-submit test_1.py)

python test_1.py

```