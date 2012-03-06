require 'test_helper'

class TestRunControllerTest < ActionController::TestCase
  test "should get run" do
    get :run
    assert_response :success
  end

end
