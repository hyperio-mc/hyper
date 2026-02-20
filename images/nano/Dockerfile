FROM denoland/deno:debian

WORKDIR /app

# Copy files
COPY . .

# Run the app
EXPOSE 8080
CMD ["deno", "run", "--allow-all", "mod.js", "--experimental"]
