from django.apps import AppConfig


class JobliteConfig(AppConfig):
    name = "joblite_django"
    verbose_name = "joblite"

    def ready(self):
        # Touching db() here would open the sqlite file in every worker
        # process, including in short-lived management commands. We let it
        # open lazily instead.
        pass
