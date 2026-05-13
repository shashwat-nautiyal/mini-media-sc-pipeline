FROM ruby:3.2-slim

# Install OS dependencies required by native extensions (e.g., ruby-kafka)
RUN apt-get update -qq && apt-get install -y build-essential

WORKDIR /app

COPY Gemfile* ./
RUN bundle install --without development test

COPY . .

EXPOSE 4567

CMD ["ruby", "app.rb"]