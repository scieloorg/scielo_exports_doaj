from unittest import TestCase, mock

from exporter import config


class ConfigTest(TestCase):
    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    def test_get_doaj_api_key(self):
        self.assertEqual(config.get("DOAJ_API_KEY"), "doaj-api-key-1234")

    def test_get_default_doaj_api_url_if_no_envvar_set(self):
        self.assertEqual(
            config.get("DOAJ_API_URL"), config._default.get("DOAJ_API_URL")
        )
