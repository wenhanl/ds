require 'test_helper'

class MediaControllerTest < ActionController::TestCase
  test "should get monitor" do
    get :monitor
    assert_response :success
  end

end
