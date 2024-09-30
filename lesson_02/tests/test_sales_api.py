import os
import unittest
from unittest.mock import patch, Mock

from dotenv import load_dotenv
from requests.exceptions import HTTPError
from lesson_02.job1.dal.sales_api import get_sales

load_dotenv()
AUTH_TOKEN = os.getenv("AUTH_TOKEN")

if not AUTH_TOKEN:
    print("AUTH_TOKEN environment variable must be set")

API_URL = "https://fake-api-vycpfa6oca-uc.a.run.app/sales"


class GetSalesTestCase(unittest.TestCase):
    @patch('requests.get')
    def test_get_sales_success(self, mock_get):
        """
        Test the get_sales function when the API responds successfully.
        """

        # Mocking the response for page 1 and page 2
        mock_response_page_1 = Mock()
        mock_response_page_1.status_code = 200
        mock_response_page_1.json.return_value = [
            {"id": 1, "sale": 100}, {"id": 2, "sale": 200}
        ]

        mock_response_page_2 = Mock()
        mock_response_page_2.status_code = 200
        mock_response_page_2.json.return_value = [{"id": 3, "sale": 300}]

        mock_response_end = Mock()
        mock_response_end.status_code = 404
        mock_response_end.json.return_value = []

        # Simulate the sequential call for different pages
        mock_get.side_effect = [
            mock_response_page_1, mock_response_page_2, mock_response_end
        ]

        # Call the get_sales function
        sales_data = get_sales("2023-09-20")

        # Verify the correct requests were made
        mock_get.assert_any_call(
            url=API_URL,
            params={"date": "2023-09-20", "page": 1},
            headers={"Authorization": os.getenv("AUTH_TOKEN")},
        )
        mock_get.assert_any_call(
            url=API_URL,
            params={"date": "2023-09-20", "page": 2},
            headers={"Authorization": os.getenv("AUTH_TOKEN")},
        )

        # Check if the final data matches the combined mock data
        expected_data = [
            {"id": 1, "sale": 100},
            {"id": 2, "sale": 200},
            {"id": 3, "sale": 300}
        ]
        self.assertEqual(sales_data, expected_data)

    @patch('requests.get')
    def test_get_sales_http_error(self, mock_get):
        """
        Test the get_sales function when the API returns an HTTP error.
        """

        # Mocking an HTTP error response
        mock_get.side_effect = HTTPError("API error")

        with self.assertRaises(SystemExit):
            get_sales("2023-09-20")

        # Verify that the API call was made once before raising the exception
        mock_get.assert_called_once_with(
            url=API_URL,
            params={"date": "2023-09-20", "page": 1},
            headers={"Authorization": os.getenv("AUTH_TOKEN")},
        )
