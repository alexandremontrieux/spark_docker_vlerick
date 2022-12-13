FROM python:3.8

pip install -r requirements.txt

# Install PySpark and Pandas
COPY requirements.txt /app/

# Copy the necessary files into the image
COPY main.py /app/
COPY credentials.json /app/

# Set the working directory
WORKDIR /app

# Run the code to read the CSV data from the S3 bucket, convert the data to a Pandas DataFrame,
# run the ML training and scoring logic, and write the results to the S3 bucket
CMD ["python", "main.py"]