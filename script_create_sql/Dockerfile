FROM python


WORKDIR /app

COPY script.py /app/script.py
COPY analyze_sql.py /app/analyze_sql.py

RUN pip install pandas requests

CMD [ "bash", "-c", "python script.py && analyze_sql.py && bash" ]