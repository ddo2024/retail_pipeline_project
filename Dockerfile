FROM python:3.10-slim

# Install basic dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install dbt-core and dbt-postgres
RUN pip install --no-cache-dir \
    dbt-core==1.6.9 \
    dbt-postgres==1.6.9 \

    pandas \
    sqlalchemy \
    pymysql \
    psycopg2-binary \
    cryptography 
  


# Create a working directory
WORKDIR /usr/app

# Default command       
CMD ["dbt", "--help"]
