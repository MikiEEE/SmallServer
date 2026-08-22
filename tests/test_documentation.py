import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
GUIDE_FILES = {
    "index.md",
    "getting-started.md",
    "routing.md",
    "requests-and-responses.md",
    "runtime-lifecycle.md",
    "configuration.md",
    "adapters.md",
    "errors-observability.md",
    "platforms-kernels.md",
    "api-reference.md",
    "protocol-roadmap.md",
    "development.md",
}
MARKDOWN_LINK = re.compile(r"(?<!!)\[[^]]+\]\(([^)]+)\)")
PYTHON_BLOCK = re.compile(r"```python\n(.*?)```", re.DOTALL)
HEADING = re.compile(r"^#{1,6}\s+(.+)$", re.MULTILINE)


def heading_slug(value):
    value = re.sub(r"[^\w\- ]", "", value.strip().lower())
    return value.replace(" ", "-")


class DocumentationTests(unittest.TestCase):
    def _documents(self):
        yield ROOT / "README.md"
        yield from sorted((ROOT / "guide").glob("*.md"))

    def test_public_guide_has_the_expected_pages(self):
        self.assertEqual(
            {path.name for path in (ROOT / "guide").glob("*.md")}, GUIDE_FILES
        )

    def test_relative_markdown_links_resolve(self):
        failures = []
        for document in self._documents():
            for target in MARKDOWN_LINK.findall(document.read_text()):
                path_text, separator, fragment = target.partition("#")
                if not path_text or "://" in path_text or path_text.startswith("mailto:"):
                    continue
                destination = (document.parent / path_text).resolve()
                if not destination.exists():
                    failures.append("{} -> {}".format(document.relative_to(ROOT), target))
                    continue
                if separator:
                    headings = {
                        heading_slug(value)
                        for value in HEADING.findall(destination.read_text())
                    }
                    if fragment not in headings:
                        failures.append(
                            "{} -> {} (missing heading)".format(
                                document.relative_to(ROOT), target
                            )
                        )
        self.assertEqual(failures, [])

    def test_python_code_blocks_compile(self):
        failures = []
        for document in self._documents():
            for position, source in enumerate(PYTHON_BLOCK.findall(document.read_text()), 1):
                try:
                    compile(source, "{}:block{}".format(document, position), "exec")
                except SyntaxError as exc:
                    failures.append(str(exc))
        self.assertEqual(failures, [])


if __name__ == "__main__":
    unittest.main()
