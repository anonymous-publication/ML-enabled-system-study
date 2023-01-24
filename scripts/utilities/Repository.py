from distutils.log import error
import json
from threading import local
import numpy as np
import git
import ast
import os
import pandas as pd
import subprocess
import shutil
import platform
from treelib import Node, Tree
import pydriller
import nbformat as nbf
from nbconvert.exporters import PythonExporter
from nbconvert.preprocessors import TagRemovePreprocessor
import paramiko
import tarfile
import pysftp
from stat import S_ISDIR, S_ISREG


class FileNotReadableError(Exception):
    """Raised when a file could not be processed for analysis. Used only within the context of the Repository class."""
    pass


class Repository:
    """Contains functions to perform analysis on a repository"""

    def __init__(self, remote_url='', local_dir='', sftp_compressed='',sftp_uncompressed=''):
        """
        Args:
            remote_url:         Remote address to clone the repository from.
            local_dir:          Local directory where the repository is stored.
            sftp_compressed:    Url of a tar-compressed file containing the repository on an sftp server
            sftp_uncompressed:  Url of a folder containing the repository on an aftp server

            If only remote_url is provided, the repository is cloned into a temporary local folder and deleted upon destruction of the object. If only local_dir is procided, the repository is constructed based on the local copy that is already downloaded. If both are provided, the repository is cloned to the specified folder and kept after destruction of the object.
            Alternatively, only sftp_compressed can be provided in order to download the repo from an sftp server and temporarily store the extracted version locally.
        """

        if remote_url and local_dir:
            # Clone the repository and save it permanently in the specified local folder
            self.delete_when_done = False
            self.local_dir = local_dir

            # Check if local copy already exists
            if not os.path.exists(local_dir):
                self.clone(remote_url, local_dir)

        elif remote_url:
            # Clone the repository into a temporary local folder
            self.delete_when_done = True
            repo_name = remote_url[19:].replace('/', '-')
            # self.local_dir = os.path.abspath('./tmp/{}'.format(repo_name))
            self.local_dir = os.path.join(os.getcwd(), 'tmp', repo_name)
            self.clone(remote_url, self.local_dir)

        elif local_dir:
            # Create repository from local folder
            self.delete_when_done = False
            if os.path.exists(local_dir):
                self.local_dir = local_dir
            else:
                raise ValueError("Local directory does not exist.")

        elif sftp_compressed:
            self.delete_when_done = True
            repo_name = sftp_compressed.split('/')[-1]
            self.local_dir = os.path.join(os.getcwd(), 'tmp', repo_name)
            self.download_tar(sftp_compressed)

        elif sftp_uncompressed:
            self.delete_when_done = True
            repo_name = sftp_compressed.split('/')[-1]
            self.local_dir = os.path.join(os.getcwd(), 'tmp', repo_name)
            self.download(sftp_compressed)

        else:
            raise ValueError(
                "At least one of remote_url, local_dir, sftp_compressed or sftp_uncompressed has to be provided.")

    def __del__(self):
        if self.delete_when_done:
            try:
                shutil.rmtree(self.local_dir)
            except:
                pass
            # Deleting temporary directory
            # try:
            #     # self.test_repo.close()
            #     git.rmtree(self.local_dir)
            #     self.test_repo.close()
            # except (AttributeError, FileNotFoundError):
            #     pass

    def clone(self, remote_url, local_dir):
        """Clone the repository from remote_url and save in local foler."""
        # # Make sure the directory is empty
        # if os.path.exists(self.local_dir):
        #     try:
        #         shutil.rmtree(self.local_dir)
        #     except:
        #         raise ValueError(
        #             "An error occured when trying to clone the repository. Skipping repo.")

        if platform.system() == 'Linux':
            # Clone the repository using terminal command
            command = "git clone {} {}".format(remote_url, self.local_dir)
            proc = subprocess.Popen(
                [command], stdout=subprocess.PIPE, shell=True)
            # In case that the repository requires authentication, an authentication prompt is opened in the terminal. Without user input this should trigger a timeout and the repository is skipped.
            try:
                outs, errs = proc.communicate(timeout=60)
            except:
                proc.kill()
                raise ValueError(
                    "An error occured when trying to clone the repository. Skipping repo.")
        elif platform.system() == 'Windows':
            # Clone the repository using gitpython
            try:
                self.test_repo = git.Repo.clone_from(
                    remote_url, self.local_dir)
            except (git.exc.GitCommandError, UnicodeDecodeError):
                # Handling error while cloning the repository
                raise ValueError(
                    "An error occured when trying to clone the repository. Skipping repo.")
        else:
            print("This makes no sense")
            print(platform.system())

    def download_tar(self, repo_name):
        """Downloads and extracts a tar compressed repository from an sftp server"""

        #Adapt this to user-specific environment
        host = 'se-fs1.ig09s.ruhr-uni-bochum.de'
        port = 9022

        transport = paramiko.Transport((host, port))

        with open('fileserver_username.txt') as f:
            username = f.readline()
        with open('fileserver_password.txt') as f:
            password = f.readline()

        #Store the .tar file in a temporary local directory before extraction
        local_path = os.path.join(os.getcwd(), 'tmp_compressed', repo_name)
        #Adapt this depending on the structure of your server
        remote_path = f'mlexpmining/compressed_repos/{repo_name}'
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        try:
            sftp.get(remote_path, local_path)
        except Exception as e:
            print(e)
        transport.close()

        # Extract tarfile
        tmp_dir = './tmp'
        with tarfile.open(local_path, 'r:gz') as tar:
            try:
                tar.extractall(tmp_dir)
            except Exception as e:
                print(e)

        # Remove the compressed file
        os.remove(local_path)

    def sftp_get_recursive(self, path, dest, sftp):
        """Download an entire folder from an sftp server using paramiko"""

        item_list = sftp.listdir_attr(path)
        dest = str(dest)
        if not os.path.isdir(dest):
            os.makedirs(dest, exist_ok=True)
        for item in item_list:
            mode = item.st_mode
            if S_ISDIR(mode):
                self.sftp_get_recursive(path + "/" + item.filename,
                                        dest + "/" + item.filename, sftp)
            else:
                sftp.get(path + "/" + item.filename,
                         dest + "/" + item.filename)

    def download(self, repo_name):
        """Downloads an uncompressed repository from an sftp server"""

        host = 'se-fs1.ig09s.ruhr-uni-bochum.de'
        port = 9022

        with open('fileserver_username.txt') as f:
            username = f.readline()
        with open('fileserver_password.txt') as f:
            password = f.readline()

        transport = paramiko.Transport((host, port))
        local_path = os.path.join(os.getcwd(), 'tmp', repo_name)
        remote_path = f'mlexpmining/cloned_repos/{repo_name}'
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        try:
            self.sftp_get_recursive(remote_path, local_path, sftp)
        except Exception as e:
            print('test')
            print(e)
            input()
        sftp.close()
        transport.close()

    def get_imports(self, filepath):
        """Returns a list of libraries imported by the given file."""

        imports = []
        with open(filepath, 'r', encoding='utf-8') as source:
            try:
                # Create an abstract syntax tree
                tree = ast.parse(source.read())
                # Traverse the tree in search of import statements
                for node in ast.walk(tree):
                    # Check for Import
                    if isinstance(node, ast.Import):
                        names = node.names
                        for name in names:
                            imports.append(name.name)
                    # Check for ImportFrom
                    elif isinstance(node, ast.ImportFrom):
                        if node.module is None:
                            continue
                        # Append the module from which is imported
                        imports.append(node.module)
            except SyntaxError as e:
                # Handling files with syntax errors
                raise e

        return imports

    def get_all_imports(self):
        """ Returns a list with all imports for this repository."""

        imports = []
        # Keep track of how many files were skipped
        total_files = 0
        skipped_files = 0

        # Iterate through the files in the repository
        for root, directories, files in os.walk(self.local_dir):
            for f in files:
                filepath = ""
                # Search Python source files for imports
                if (f.endswith('.py')):
                    total_files += 1
                    filepath = os.path.join(root, f)
                elif (f.endswith('.ipynb')):
                    total_files += 1
                    try:
                        filepath = self._convert_notebook(
                            os.path.join(root, f))
                    except FileNotReadableError:
                        skipped_files += 1

                # For .py files and successfully converted notebooks
                if filepath and os.path.exists(filepath):
                    try:
                        imports += self.get_imports(filepath)
                    except (SyntaxError, FileNotFoundError) as e:
                        print("Syntax error. Skipping file")
                        skipped_files += 1

        # Removing duplicates
        imports = list(dict.fromkeys(imports))
        print("Skipped {} out of {} files".format(skipped_files, total_files))

        return imports

    def _visit_node(self, node):
        if type(node) == ast.Call:
            return self._visit_node(node.func)
        elif type(node) == ast.Attribute:
            return node.attr
        if type(node) == ast.Name:
            return node.id

    def _convert_notebook(self, filepath: str):
        """Converts the notebook to a .py file and returns the filepath to the converted notebook.
        Note: Make sure that nbconvert is installed! Otherwise no error will be raised, all notebook files are just skipped."""

        # Convert to .py
        os.system(
            f'jupyter nbconvert \"{filepath}\" --to python')
        converted_file_path = filepath[:-5]+'py'
        # Remove notebook file to save time in future analysis
        try:
            os.remove(filepath)
        except PermissionError:
            pass
        # If conversion was successfull, return the path to the new file
        if os.path.exists(converted_file_path):
            return converted_file_path
        else:
            raise FileNotReadableError("Error converting Jupyter notebook.")

    def _process_ast(self, tree, functions_to_stages):
        """Returns a list of ml stages for an abstract syntax tree"""
        implemented_stages = []
        # Iterate over the ast and check for all function calls if they are part of a stage of ml workflow
        for node in ast.walk(tree):
            if type(node) == ast.Call:
                function_name = self._visit_node(node)
                if function_name:
                    try:
                        stage = functions_to_stages[function_name]
                        implemented_stages.append(stage)
                    except KeyError:
                        pass
        return np.unique(implemented_stages)

    def _process_script(self, filepath: str, functions_to_stages: dict):
        """Returns a list of ml stages implemented py a python source file."""

        try:
            with open(filepath, 'r', encoding='utf-8') as source:
                # Create an abstract syntax tree
                tree = ast.parse(source.read())
        except Exception as e:
            # Expect SyntaxError, FileNotFoundError, PermissionError, OSError and maybe more
            print(f"{type(e).__name__}. Skipping file")
            self.skipped_files += 1
            return[]

        implemented_stages = self._process_ast(tree, functions_to_stages)
        return implemented_stages

    def get_ml_stages(self, stages_to_functions, functions_to_stages):
        """
        Returns a list of files for each stage of ml workflow.
        """
        # Keeping track of how many files were skipped
        self.skipped_files = 0
        stages_to_files = {x: []for x in stages_to_functions}
        # Iterate through all files and determine their stages
        for root, directories, files in os.walk(self.local_dir):
            for f in files:
                # Get the stages implemented by this file
                filepath = os.path.join(root, f)
                ml_stages = []
                # Call _process_script to get the ml stages implemented by this file
                if (f.endswith('.ipynb')):
                    # For Jupyter notebooks: Try converting them to .py files first
                    try:
                        converted_file_path = self._convert_notebook(filepath)
                        ml_stages = self._process_script(
                            converted_file_path, functions_to_stages)
                    except FileNotReadableError as e:
                        print(e)
                elif (f.endswith('.py')):
                    ml_stages = self._process_script(
                        filepath, functions_to_stages)
                # Add files to stages_to files
                for stage in ml_stages:
                    stages_to_files[stage].append(filepath)

        return stages_to_files

    def get_file_tree(self, tree=None, node=None, path=None):
        """Create a tree of the file structure. Uses recursion to iterate through the local directory. Parameters are only required in recursive calls."""

        # Initialization
        if tree is None:
            tree = Tree()
            root_name = self.local_dir.split('\\')[-1]
            node = tree.create_node(root_name, 'root').identifier
            path = self.local_dir
        # Base case
        elif os.path.isfile(path):
            return tree

        # Iterate through the files in the directory and append them to the tree
        for i in os.listdir(path):
            new_node = tree.create_node(i, parent=node)
            new_path = os.path.join(path, i)
            tree = self.get_file_tree(tree, new_node, new_path)

        # After appending all subtrees return the resulting tree
        return tree

    def _parse_notebook(self, notebook):
        """Parses a Jupyter notebook into an abstract syntax tree(ast)."""
        try:
            notebook_nodes = nbf.reads(notebook, as_version=4)
        except Exception:
            # Catch any errors caused by non-readable notebooks
            return None
        trp = TagRemovePreprocessor()
        trp.remove_cell_tags = ("remove",)
        pexp = PythonExporter()
        pexp.register_preprocessor(trp, enabled=True)
        try:
            the_python_script, meta = pexp.from_notebook_node(notebook_nodes)
            return ast.parse(the_python_script)
        except Exception:
            # Expect syntax error during parsing or other errors during notebook conversion
            return None

    def get_commit_stages(self, functions_to_stages: dict, stages: list):
        """
        Calculate the number of files in each ml stages affected by each commit.
        Returns: pd.Dataframe with one row for each commit, one column for each stage and one for the timestamp.
        """
        # Create list for results
        result_list = []
        # Create base class for pydriller
        self.pyd_repo = pydriller.Repository(self.local_dir)

        # Iterate through commits
        for commit in self.pyd_repo.traverse_commits():
            errors = 0
            # Create a new line for the result-dataframe
            new_line = {stage: 0 for stage in stages}
            new_line['time'] = commit.author_date
            # Get ml stages for each modified source file
            for file in commit.modified_files:
                # Create ast for scripts and notebooks
                old_tree = None
                new_tree = None
                # Convert and parse notebooks
                if file.filename.endswith('.ipynb'):
                    try:
                        old_tree = self._parse_notebook(
                            file.source_code_before)
                    except Exception:
                        # TypeError occurs if the file was created in this commit
                        pass
                    try:
                        new_tree = self._parse_notebook(
                            file.source_code)
                    except Exception:
                        # TypeError occurs if the file was deleted in this commit
                        pass
                # Parse python source files
                elif file.filename.endswith('.py'):
                    try:
                        old_tree = ast.parse(file.source_code_before)
                    except Exception:
                        # TypeError occurs if the file was created in this commit
                        pass
                    try:
                        new_tree = ast.parse(file.source_code)
                    except Exception:
                        # TypeError occurs if the file was deleted in this commit
                        pass

                # Extract ml stages from ast
                implemented_stages = None
                if old_tree is not None and new_tree is not None:
                    implemented_stages = np.concatenate((self._process_ast(
                        old_tree, functions_to_stages), self._process_ast(
                        new_tree, functions_to_stages)))
                elif new_tree is not None:
                    implemented_stages = self._process_ast(
                        new_tree, functions_to_stages)
                elif old_tree is not None:
                    implemented_stages = self._process_ast(
                        old_tree, functions_to_stages)

                # Append the information from this file to the new line
                if implemented_stages is not None:
                    for stage in set(implemented_stages):
                        new_line[stage] += 1

            # Append the results for this commit to the result-dataframe
            result_list.append(new_line)

        return pd.DataFrame(result_list)

    def get_ml_files(self,stages_to_functions,functions_to_stages):
        '''Returns a list of files implementing an ml stage.'''

        #Get files implementing an ml stage
        ml_stages_dict=self.get_ml_stages(stages_to_functions,functions_to_stages)
        ml_stages_list=[]
        #Convert the dictionary into a list of files
        for v in ml_stages_dict.values():
            ml_stages_list+=v
        #Remove duplicates
        ml_stages_set=set(ml_stages_list)

        return ml_stages_set
