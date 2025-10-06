import json
import logging
from datetime import datetime
from typing import Dict, Any


def format_args(args, yaml_style=False):
    """
    Formats the args dictionary into the desired output format.

    Args:
        args (dict): A dictionary of variable names and their values.
        yaml_style (bool): If True, formats the output in YAML-style.

    Returns:
        str: The formatted string.
    """
    formatted_args = []

    for key, value in args.items():
        if value is None or value == "":
            formatted_value = ""
        elif isinstance(value, str):
            formatted_value = f'"{value}"'
        elif isinstance(value, dict):
            formatted_value = json.dumps(value)  # Convert dict to JSON format
        else:
            formatted_value = value

        if yaml_style:
            formatted_args.append(f" :{key}: {formatted_value}")
        else:
            formatted_args.append(f"    {key}: {formatted_value}")

    if yaml_style:
        return "\n".join(f"-{formatted_args[0]}" if i == 0 else f" {line}" for i, line in enumerate(formatted_args))

    return "\n".join(formatted_args)


def generate_handler(classname: str, args: Dict[str, Any], use_class_object: bool = True, method_name: str ='run') -> str:
    """
    Generates a handler string in the Delayed::PerformableMethod format.

    Args:
        classname (str): The class name to be used in the handler.
        args (Dict[str, Any]): A dictionary of arguments where keys are variable names and values are their corresponding values.
        use_class_object (bool): Determines whether to use an object or a class reference.

    Returns:
        str: The formatted handler string.
    """
    # Generate the args section dynamically
    formatted_args = format_args(args, yaml_style=(use_class_object==False))

    if use_class_object:
        return f"""--- !ruby/object:Delayed::PerformableMethod
object: !ruby/object:{classname}
  raw_attributes:
{formatted_args}
"""
    else:

        return f"""--- !ruby/object:Delayed::PerformableMethod
object: !ruby/class '{classname}'
method_name: :{method_name}
args:
{formatted_args}
"""


class DelayedJobHandler:
    def __init__(self, database, logger=None):
        """
        Initializes the DelayedJobHandler with a database connection and logger.

        :param database: A database connection object.
        :param logger: A logger object (optional). If not provided, a default logger will be used.
        """
        self.database = database
        self.logger = logger or logging.getLogger(__name__)

    def get_current_time(self):
        """
        Helper function to get the current time.

        :return: The current time as a datetime object.
        """
        return datetime.now()

    def create_delayed_job(
        self,
        classname: str,
        args: Dict[str, Any],
        queue: str = "default",
        use_class_object: bool = True,
        method_name: str = "run"
    ) -> None:
        """
        Creates a delayed job with the given class and arguments.

        Args:
            classname (str): The name of the class to be used in the handler.
            args (Dict[str, Any]): A dictionary of arguments where keys are variable names and values are their corresponding values.
            queue (str, optional): The queue name where the job should be enqueued. Defaults to "default".
            use_class_object (bool, optional): Determines whether the object should be instantiated (`True`) or referenced as a class (`False`). Defaults to `True`.
            method_name (str, optional): The method to be invoked on the class. Defaults to `"run"`.

        Returns:
            None
        """

        # Generate the handler string dynamically
        handler = generate_handler(
            classname=classname,
            args=args,
            use_class_object=use_class_object,
            method_name=method_name
        )
        # SQL query to insert the delayed job
        query = '''
        INSERT INTO delayed_jobs
            (handler, queue, run_at, created_at, updated_at)
        VALUES
            (%s, %s, %s, %s, %s)
        '''

        # Log the query for debugging
        self.logger.debug(f"Insert delayed job query: {query}")
        self.logger.debug(f"Handler: {handler}")
        self.logger.debug(f"queue: {queue}")
        # Get the current time
        now = self.get_current_time()

        # Execute the query
        cursor = self.database.cursor()
        cursor.execute(query, (handler, queue, now, now, now))

        # Commit the transaction
        self.database.commit()