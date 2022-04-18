import great_expectations as ge
import importlib.util
spec_unique = importlib.util.spec_from_file_location("expect_non_empty_unique", "./great_expectations/plugins/custom_modules/expect_non_empty_unique.py")
expect_non_empty_unique = importlib.util.module_from_spec(spec_unique)
spec_unique.loader.exec_module(expect_non_empty_unique)
spec_list = importlib.util.spec_from_file_location("expect_type_list", "./great_expectations/plugins/custom_modules/expect_type_list.py")
expect_type_list = importlib.util.module_from_spec(spec_list)
spec_list.loader.exec_module(expect_type_list)

"""
The actual Great Expectations cli commands cannot handle custom expectations. So this file exists just to import these
custom files before running the Great Expectations validation.
"""

if __name__ == "__main__":
    context = ge.get_context()
    context.run_checkpoint(checkpoint_name="main-val")