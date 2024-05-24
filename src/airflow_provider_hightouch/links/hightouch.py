from airflow.models import BaseOperatorLink


class HightouchLink(BaseOperatorLink):
    name = "Hightouch"

    def get_link(self, operator, task_instance):
        return "https://app.hightouch.io"
