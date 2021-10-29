require 'json'

def sessions_analytics(data)
  analytics = {}
  user_sessions = {}
  users = data[:events].sort { |obj1, obj2| obj1[:timestamp] <=> obj2[:timestamp] }
                       .group_by { |event| event[:visitorId].itself }
  users.keys.each do |user|
    prec_start_time = 0
    sessions = []
    session = {
      duration: 0,
      pages: [],
      startTime: nil
    }
    users[user].each do |event|
      if !session[:startTime]
        session[:pages] << event[:url]
        session[:startTime] = event[:timestamp]
        prec_start_time = event[:timestamp]
      else
        if event[:timestamp] - prec_start_time <= 600_000
          session[:duration] = event[:timestamp] - prec_start_time
          session[:pages] << event[:url]
        else
          sessions << session
          prec_start_time = 0
          session = {
            duration: 0,
            pages: [],
            startTime: nil
          }
          redo
        end
      end
    end
    sessions << session
    user_sessions[user] = sessions
  end
  analytics[:sessionsByUser] = user_sessions
  analytics
end

file   = File.read('./input.json')
input  = JSON.parse(file, symbolize_names: true)
output = sessions_analytics(input)
File.write('./output.json', JSON.dump(output))
