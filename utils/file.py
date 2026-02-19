# utils/file_filters.py

def is_test_file(file_path, filename):
    """
    Common filter to identify test, sample, and mock files across C, C++, and Java.
    
    :param file_path: The full path of the file (usually m.new_path or m.old_path)
    :param filename: The name of the file (usually m.filename)
    :return: Boolean (True if it's a test/sample file)
    """
    if not file_path:
        return False
        
    path_lower = file_path.lower()
    name_lower = filename.lower()

    test_dirs = [
        "/test/", "/tests/", "/testing/", "/unit_test/", 
        "/unit_tests/", "/samples/", "/examples/", "src/test/"
    ]
    if any(td in path_lower for td in test_dirs):
        return True

    if name_lower.startswith("test_"):
        return True

    test_suffixes = (
        "_test.c", "_test.cpp", "_test.cc", "_test.h", "_test.hpp",
        "test.java", "it.java", "test.cpp", "tests.cpp"
    )
    if name_lower.endswith(test_suffixes):
        return True
        
    if ".test." in path_lower or "mock" in name_lower:
        return True

    return False

def is_source_code(filename, language=None):
    """
    Filters for relevant source code extensions.
    """
    name_lower = filename.lower()
    cpp_exts = (".c", ".cpp", ".cc", ".h", ".hpp")
    java_exts = (".java")
    
    if language == "Java":
        return name_lower.endswith(java_exts)
    elif language in ["C", "C++"]:
        return name_lower.endswith(cpp_exts)
    
    return name_lower.endswith(cpp_exts + (java_exts,))