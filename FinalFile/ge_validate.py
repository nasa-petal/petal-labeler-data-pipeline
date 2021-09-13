import great_expectations as ge
import importlib.util
spec = importlib.util.spec_from_file_location("expect_non_empty_unique", "./great_expectations/plugins/custom_modules/expect_non_empty_unique.py")
expect_non_empty_unique = importlib.util.module_from_spec(spec)
spec.loader.exec_module(expect_non_empty_unique)

"""
The actual Great Expectations cli commands cannot handle custom expectations. So this file exists just to import these
custom files before running the Great Expectations validation.
"""
if __name__ == "__main__":
    context = ge.get_context()
    context.run_checkpoint(checkpoint_name="main-val")