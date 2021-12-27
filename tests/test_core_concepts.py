import unittest
from airflow.models import DagBag

class TestCoreConceptsDAG(unittest, TestCase):

    def setup(self):
        self.dagbag = DagBag()
        self.dag = self.dagbag.get_dag(dag_id = "core_concepts")

    # check for errors in the dag while loading
    def test_dag_loaded(self):
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(self.dag)

    # check for the contents of a specific tasks
    def test_contain_tasks(self):
        self.assertListEqual(self.dag.task_ids, ['bash_command', 'python_function'])

    # check to test the dependencies of the specific tasks in a dag
    def test_dependencies_of_bash_command(self):
        bash_task = self.dag.get_task('bash_command')

        self.assertEqual(bash_task.upstream_tasks_ids, set())
        self.assertEqual(bash_task.downstream_tasks_ids, set(['python_function']))

    # testing the whoel struture of the tests
    # By iterating through and checking the upstream and downstream of the tasks
    def assertDagDictEqual(self, structure, dag):

        # structure is the dictionary
        self.assertEqual(dag.task_dict.keys(), structure.keys())

        for task_id, downstream_list in structure.items():
            self.assertTrue(dag.has_task(task_id))

            task = dag.get_task(task_id)
            self.assertEqual(task.downstream_task_ids, set(downstream_list))

    def test_dag_structure(self):
        self.assertDagDictEqual(
            {
                'bash_command': ['python_function'],
                'python_function': []
            },
            self.dag
        )

